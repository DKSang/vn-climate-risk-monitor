"""Plan or run incremental Open-Meteo Archive ingestion into Bronze."""

from __future__ import annotations

import argparse
import calendar
import os
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.collectors.open_meteo_archive import (
    ArchiveCollectionResult,
    ArchiveCollector,
    effective_call_units,
)
from vn_climate_risk_monitor.ingestion.http import build_http_session
from vn_climate_risk_monitor.ingestion.loaders.open_meteo_archive_hourly import (
    ArchiveHourlyLoader,
    LoadFailure,
    LoadSummary,
)
from vn_climate_risk_monitor.ingestion.open_meteo import (
    ARCHIVE_HOURLY_VARIABLES,
    DEFAULT_ARCHIVE_START_YEAR,
    ArchiveYearPlan,
    latest_complete_archive_date,
    plan_archive_backfill,
    plan_archive_year,
)
from vn_climate_risk_monitor.ingestion.open_meteo.locations import (
    load_hanoi_locations,
)
from vn_climate_risk_monitor.ingestion.state import (
    PostgresIngestionRepository,
    RunAlreadySucceededError,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import (
    ImmutableObjectWriter,
    VerifiedObjectReader,
    ensure_bucket,
    get_minio_client,
)


class CollectionOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    ALREADY_SUCCEEDED = "ALREADY_SUCCEEDED"


class Collector(Protocol):
    def collect_year(
        self, year_plan: ArchiveYearPlan, **values: object
    ) -> ArchiveCollectionResult: ...


class Loader(Protocol):
    def load_available(self, **values: object) -> LoadSummary: ...


@dataclass(frozen=True)
class ArchivePipelineSummary:
    logical_key: str
    scope: str
    collection_outcome: CollectionOutcome
    attempt_id: UUID | None
    load_batches: int
    claimed_files: int
    committed_files: int
    rows_parsed: int
    rows_inserted: int
    rescued_rows: int
    failures: tuple[LoadFailure, ...]


class ArchivePipeline:
    """Collect one year/date partition, then drain its file checkpoints."""

    def __init__(self, *, collector: Collector, loader: Loader) -> None:
        self.collector = collector
        self.loader = loader

    def run(
        self,
        year_plan: ArchiveYearPlan,
        *,
        scheduled_at_utc: datetime,
        scope: str,
        worker_id: str,
        loader_batch_size: int,
        lease_seconds: int,
        max_retries: int,
        max_load_batches: int,
    ) -> ArchivePipelineSummary:
        if min(loader_batch_size, lease_seconds, max_load_batches) < 1:
            raise ValueError("Pipeline batch and lease limits must be positive")

        attempt_id: UUID | None = None
        try:
            collection = self.collector.collect_year(
                year_plan,
                scheduled_at_utc=scheduled_at_utc,
                scope=scope,
            )
            outcome = CollectionOutcome.SUCCEEDED
            attempt_id = collection.attempt.attempt_id
        except RunAlreadySucceededError:
            # Scheduler retries do not call the source again. The loader still
            # drains source files whose Bronze checkpoint is incomplete.
            outcome = CollectionOutcome.ALREADY_SUCCEEDED

        load_batches = 0
        claimed_files = 0
        committed_files = 0
        rows_parsed = 0
        rows_inserted = 0
        rescued_rows = 0
        failures: list[LoadFailure] = []
        for _ in range(max_load_batches):
            batch = self.loader.load_available(
                scope=scope,
                worker_id=worker_id,
                limit=loader_batch_size,
                lease_seconds=lease_seconds,
                max_retries=max_retries,
            )
            if batch.claimed_files == 0:
                break
            load_batches += 1
            claimed_files += batch.claimed_files
            committed_files += batch.committed_files
            rows_parsed += batch.rows_parsed
            rows_inserted += batch.rows_inserted
            rescued_rows += batch.rescued_rows
            failures.extend(batch.failures)
            if batch.failures:
                break
        else:
            raise RuntimeError(
                "Archive loader did not drain within OPEN_METEO_MAX_LOAD_BATCHES"
            )

        return ArchivePipelineSummary(
            logical_key=year_plan.logical_key,
            scope=scope,
            collection_outcome=outcome,
            attempt_id=attempt_id,
            load_batches=load_batches,
            claimed_files=claimed_files,
            committed_files=committed_files,
            rows_parsed=rows_parsed,
            rows_inserted=rows_inserted,
            rescued_rows=rescued_rows,
            failures=tuple(failures),
        )


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected ISO date YYYY-MM-DD") from error


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected ISO timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("scheduled-at must include a UTC offset")
    return parsed.astimezone(UTC)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _month(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 12:
        raise argparse.ArgumentTypeError("month must be between 1 and 12")
    return parsed


def _selected_plans(
    *,
    locations: tuple,
    model: str,
    location_batch_size: int,
    available_through: date,
    start_year: int,
    end_date: date | None,
    year: int | None,
    month: int | None,
    tail: bool,
) -> tuple[ArchiveYearPlan, ...]:
    if tail:
        return (
            plan_archive_year(
                locations,
                start_date=available_through,
                end_date=available_through,
                model=model,
                location_batch_size=location_batch_size,
                hourly_variables=ARCHIVE_HOURLY_VARIABLES,
            ),
        )
    if year is not None:
        if year > available_through.year:
            raise ValueError(f"year {year} is later than available data")
        if month is not None:
            start_date = date(year, month, 1)
            selected_end = min(
                date(year, month, calendar.monthrange(year, month)[1]),
                available_through,
            )
            if selected_end < start_date:
                raise ValueError(
                    f"source period starts after available date {available_through}"
                )
            return (
                plan_archive_year(
                    locations,
                    start_date=start_date,
                    end_date=selected_end,
                    model=model,
                    location_batch_size=location_batch_size,
                    hourly_variables=ARCHIVE_HOURLY_VARIABLES,
                ),
            )
        year_end = min(date(year, 12, 31), available_through)
        return _monthly_run_plans(
            locations=locations,
            start_date=date(year, 1, 1),
            end_date=year_end,
            model=model,
            location_batch_size=location_batch_size,
        )

    selected_end = end_date or available_through
    if selected_end > available_through:
        raise ValueError(
            f"end date {selected_end} exceeds archive availability "
            f"{available_through}"
        )
    yearly_plans = plan_archive_backfill(
        locations,
        start_year=start_year,
        end_date=selected_end,
        model=model,
        location_batch_size=location_batch_size,
        hourly_variables=ARCHIVE_HOURLY_VARIABLES,
    ).years
    return tuple(
        monthly
        for yearly in yearly_plans
        for monthly in _monthly_run_plans(
            locations=locations,
            start_date=yearly.start_date,
            end_date=yearly.end_date,
            model=model,
            location_batch_size=location_batch_size,
        )
    )


def _monthly_run_plans(
    *,
    locations: tuple,
    start_date: date,
    end_date: date,
    model: str,
    location_batch_size: int,
) -> tuple[ArchiveYearPlan, ...]:
    """Use monthly checkpoints while retaining year-partitioned Bronze files."""
    plans: list[ArchiveYearPlan] = []
    cursor = start_date
    while cursor <= end_date:
        month_end = min(
            date(cursor.year, cursor.month, calendar.monthrange(cursor.year, cursor.month)[1]),
            end_date,
        )
        plans.append(
            plan_archive_year(
                locations,
                start_date=cursor,
                end_date=month_end,
                model=model,
                location_batch_size=location_batch_size,
                hourly_variables=ARCHIVE_HOURLY_VARIABLES,
            )
        )
        cursor = month_end + timedelta(days=1)
    return tuple(plans)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--year", type=_positive_int)
    selection.add_argument(
        "--tail",
        action="store_true",
        help="Ingest only the latest conservatively available archive day.",
    )
    parser.add_argument("--month", type=_month)
    parser.add_argument(
        "--start-year", type=_positive_int, default=DEFAULT_ARCHIVE_START_YEAR
    )
    parser.add_argument("--end-date", type=_parse_date)
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        help="UTC planning date used to calculate the five-day availability lag.",
    )
    parser.add_argument("--limit", type=_positive_int, help="Canary location count.")
    parser.add_argument(
        "--max-periods",
        type=_positive_int,
        default=1,
        help="Maximum new monthly checkpoints admitted in one invocation.",
    )
    parser.add_argument("--scheduled-at", type=_parse_utc)
    parser.add_argument(
        "--verbose-plan",
        action="store_true",
        help="Print every source period instead of a compact plan sample.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Collect and load; default only prints deterministic plans.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.month is not None and args.year is None:
        parser.error("--month requires --year")
    if args.end_date is not None and (args.year is not None or args.tail):
        parser.error("--end-date only applies to the multi-year backfill plan")

    settings = load_settings()
    observed_at_utc = datetime.now(UTC)
    available_through = latest_complete_archive_date(
        args.as_of or observed_at_utc.date()
    )
    location_connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_hanoi_locations(location_connection)
    finally:
        location_connection.close()
    if args.limit is not None:
        locations = locations[: args.limit]
    if not locations:
        parser.error("no Hanoi locations are available")

    try:
        plans = _selected_plans(
            locations=locations,
            model=settings.open_meteo.archive_model,
            location_batch_size=settings.open_meteo.location_batch_size,
            available_through=available_through,
            start_year=args.start_year,
            end_date=args.end_date,
            year=args.year,
            month=args.month,
            tail=args.tail,
        )
    except ValueError as error:
        parser.error(str(error))
    base_scope = "tail" if args.tail else "backfill"
    scope = (
        f"{base_scope}_canary_{len(locations)}"
        if args.limit is not None
        else base_scope
    )
    print(
        "Archive ingestion plan: "
        f"scope={scope}, model={settings.open_meteo.archive_model}, "
        f"available_through={available_through}, locations={len(locations)}, "
        f"source_periods={len(plans)}, "
        f"source_files={sum(plan.expected_file_count for plan in plans)}, "
        f"expected_rows={sum(plan.expected_row_count for plan in plans)}"
    )
    displayed_plans = plans
    omitted_plan_count = 0
    if not args.verbose_plan and len(plans) > 12:
        displayed_plans = (*plans[:3], plans[-1])
        omitted_plan_count = len(plans) - len(displayed_plans)
    for plan in displayed_plans:
        reserve = effective_call_units(
            plan.tasks,
            request_attempts=settings.open_meteo.max_attempts,
        )
        print(
            f"  {plan.logical_key}: {plan.start_date}..{plan.end_date}, "
            f"files={plan.expected_file_count}, rows={plan.expected_row_count}, "
            f"worst_case_call_reserve={reserve}"
        )
    if omitted_plan_count:
        print(
            f"  ... {omitted_plan_count} periods omitted; "
            "pass --verbose-plan to print all"
        )
    if not args.execute:
        print(
            "DRY RUN: pass --execute; a backfill run admits at most "
            "--max-periods new monthly checkpoints"
        )
        return

    minio = get_minio_client(settings.minio)
    ensure_bucket(minio, settings.minio.bucket)
    control_connection = connect_control_plane(settings.postgres)
    bronze_connection = get_connection()
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    scheduled_at_utc = args.scheduled_at or observed_at_utc
    newly_collected = 0
    summaries: list[ArchivePipelineSummary] = []
    try:
        ensure_ingestion_state(control_connection)
        state = PostgresIngestionRepository(control_connection)
        with build_http_session(
            max_attempts=settings.open_meteo.max_attempts
        ) as session:
            pipeline = ArchivePipeline(
                collector=ArchiveCollector(
                    settings=settings.open_meteo,
                    session=session,
                    writer=ImmutableObjectWriter(minio, settings.minio.bucket),
                    state=state,
                ),
                loader=ArchiveHourlyLoader(
                    state=state,
                    reader=VerifiedObjectReader(minio, settings.minio.bucket),
                    bronze_connection=bronze_connection,
                ),
            )
            for plan in plans:
                summary = pipeline.run(
                    plan,
                    scheduled_at_utc=scheduled_at_utc,
                    scope=scope,
                    worker_id=worker_id,
                    loader_batch_size=settings.open_meteo.loader_batch_size,
                    lease_seconds=settings.open_meteo.loader_lease_seconds,
                    max_retries=settings.open_meteo.loader_max_retries,
                    max_load_batches=settings.open_meteo.max_load_batches,
                )
                summaries.append(summary)
                print(
                    "Archive partition result: "
                    f"logical_key={summary.logical_key}, "
                    f"collection={summary.collection_outcome}, "
                    f"committed_files={summary.committed_files}, "
                    f"rows={summary.rows_parsed}, inserted={summary.rows_inserted}, "
                    f"failed={len(summary.failures)}"
                )
                if summary.failures:
                    break
                if summary.collection_outcome == CollectionOutcome.SUCCEEDED:
                    newly_collected += 1
                    if newly_collected >= args.max_periods:
                        break
    finally:
        bronze_connection.close()
        control_connection.close()

    failures = tuple(failure for summary in summaries for failure in summary.failures)
    for failure in failures:
        print(
            f"FAILED file_id={failure.file_id} "
            f"{failure.error_type}: {failure.error_message}"
        )
    print(
        "Archive ingestion result: "
        f"partitions_checked={len(summaries)}, newly_collected={newly_collected}, "
        f"committed_files={sum(item.committed_files for item in summaries)}, "
        f"rows={sum(item.rows_parsed for item in summaries)}, "
        f"failed={len(failures)}"
    )
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
