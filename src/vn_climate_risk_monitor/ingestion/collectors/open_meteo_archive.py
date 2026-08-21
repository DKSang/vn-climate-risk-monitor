"""Collect one Open-Meteo Archive period into immutable Bronze source files."""

from __future__ import annotations

import argparse
import calendar
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol
from uuid import UUID

from requests import Session

from vn_climate_risk_monitor.config import OpenMeteoSettings, load_settings
from vn_climate_risk_monitor.ingestion.http import (
    EffectiveCallPacer,
    build_http_session,
)
from vn_climate_risk_monitor.ingestion.layout import BronzeBackfillFilesLayout
from vn_climate_risk_monitor.ingestion.open_meteo import (
    ARCHIVE_DATASET,
    COLLECTOR_VERSION,
    REQUEST_CONTRACT_VERSION,
    SOURCE_NAME,
    ArchiveRequestTask,
    ArchiveYearPlan,
    SourceObjectMetadata,
    latest_complete_archive_date,
    plan_archive_year,
)
from vn_climate_risk_monitor.ingestion.open_meteo.locations import (
    load_hanoi_locations,
)
from vn_climate_risk_monitor.ingestion.open_meteo.response import (
    received_location_count,
    response_attempt_count,
)
from vn_climate_risk_monitor.ingestion.state import (
    PostgresIngestionRepository,
    RunAlreadySucceededError,
    RunAttempt,
    RunStatus,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import (
    ImmutableObjectWriter,
    ensure_bucket,
    get_minio_client,
)

PIPELINE_NAME = "open_meteo_archive"
DATASET = ARCHIVE_DATASET


class ObjectWriter(Protocol):
    def write(
        self,
        object_key: str,
        content: bytes,
        *,
        content_type: str,
    ) -> SourceObjectMetadata: ...


class RequestPacer(Protocol):
    def wait(self, call_units: int) -> float: ...


class IngestionState(Protocol):
    def start_run(self, **values: object) -> RunAttempt: ...

    def effective_call_count(
        self,
        *,
        pipeline_name: str,
        since_utc: datetime,
    ) -> int: ...

    def register_file(
        self,
        *,
        attempt_id: UUID,
        batch_index: int,
        object_key: str,
        expected_item_count: int | None,
        file_parameters: dict[str, object],
    ) -> UUID: ...

    def record_file(
        self,
        *,
        file_id: UUID,
        metadata: SourceObjectMetadata,
        http_status: int,
        request_attempt_count: int,
    ) -> None: ...

    def validate_file(self, file_id: UUID, *, received_item_count: int) -> None: ...

    def succeed_run(self, attempt_id: UUID, *, completed_at_utc: datetime) -> None: ...

    def fail_file(self, file_id: UUID, *, error: BaseException) -> None: ...

    def fail_run(
        self,
        attempt_id: UUID,
        *,
        failed_at_utc: datetime,
        error: BaseException,
    ) -> None: ...


@dataclass(frozen=True)
class ArchiveCollectionResult:
    attempt: RunAttempt
    year_prefix: str
    response_objects: tuple[SourceObjectMetadata, ...]


def effective_call_units(
    tasks: Sequence[ArchiveRequestTask],
    *,
    request_attempts: int = 1,
) -> int:
    """Conservatively round Open-Meteo's variable/time fractions once per plan."""
    if request_attempts < 1:
        raise ValueError("request_attempts must be positive")
    units = 0.0
    for task in tasks:
        day_count = (task.window.end_date - task.window.start_date).days + 1
        time_factor = max(1.0, day_count / 14)
        variable_factor = max(1.0, len(task.hourly_variables) / 10)
        units += len(task.locations) * time_factor * variable_factor
    return math.ceil(units * request_attempts)


class ArchiveCollector:
    """Sequential period collector backed by the generic PostgreSQL ledger."""

    def __init__(
        self,
        *,
        settings: OpenMeteoSettings,
        session: Session,
        writer: ObjectWriter,
        state: IngestionState,
        pacer: RequestPacer | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if settings.concurrency != 1:
            raise ValueError("Archive collector supports concurrency=1 only")
        self.settings = settings
        self.session = session
        self.writer = writer
        self.state = state
        self.pacer = pacer or EffectiveCallPacer(
            calls_per_minute=settings.max_effective_calls_per_minute,
            calls_per_hour=settings.max_effective_calls_per_hour,
        )
        self.clock = clock or (lambda: datetime.now(UTC))

    def collect_year(
        self,
        year_plan: ArchiveYearPlan,
        *,
        scheduled_at_utc: datetime,
        scope: str = "backfill",
    ) -> ArchiveCollectionResult:
        if scheduled_at_utc.tzinfo is None or scheduled_at_utc.utcoffset() is None:
            raise ValueError("scheduled_at_utc must be timezone-aware")
        if not scope.strip():
            raise ValueError("scope must not be empty")
        if year_plan.model != self.settings.archive_model:
            raise ValueError("year plan model does not match configured archive model")
        if not year_plan.tasks:
            raise ValueError("year plan must contain request tasks")
        scheduled_at_utc = scheduled_at_utc.astimezone(UTC)
        started_at_utc = self.clock()
        if started_at_utc.tzinfo is None or started_at_utc.utcoffset() is None:
            raise ValueError("collector clock must return timezone-aware timestamps")
        started_at_utc = started_at_utc.astimezone(UTC)
        if started_at_utc < scheduled_at_utc:
            raise ValueError("collector cannot start before scheduled_at_utc")

        attempt = self.state.start_run(
            pipeline_name=PIPELINE_NAME,
            source_name=SOURCE_NAME,
            dataset=DATASET,
            scope=scope,
            logical_key=year_plan.logical_key,
            scheduled_at_utc=scheduled_at_utc,
            started_at_utc=started_at_utc,
            expected_file_count=year_plan.expected_file_count,
            source_uri=self.settings.archive_url,
            collector_version=COLLECTOR_VERSION,
            contract_version=str(REQUEST_CONTRACT_VERSION),
            run_parameters=year_plan.run_parameters.to_mapping(),
            stale_after_seconds=self.settings.collector_stale_after_seconds,
        )
        run_name = (
            f"archive_{year_plan.year}_{year_plan.end_date:%Y%m%d}_"
            f"a{attempt.attempt_number}_{attempt.attempt_id.hex[:8]}"
        )
        layout = BronzeBackfillFilesLayout("open_meteo", DATASET)
        response_objects: list[SourceObjectMetadata] = []
        active_file_id: UUID | None = None

        try:
            for task in year_plan.tasks:
                self.pacer.wait(effective_call_units((task,)))
                request = task.request_contract(
                    endpoint=self.settings.archive_url,
                    model=self.settings.archive_model,
                    requested_at_utc=self.clock(),
                )
                response_key = layout.object_key(
                    task.window.start_date,
                    run_name,
                    f"response_{task.location_batch_index:03}.json",
                )
                active_file_id = self.state.register_file(
                    attempt_id=attempt.attempt_id,
                    batch_index=task.batch_index,
                    object_key=response_key,
                    expected_item_count=len(task.locations),
                    file_parameters=task.file_parameters.to_mapping(),
                )
                response = self.session.get(
                    request.endpoint,
                    params=request.api_query_params,
                    timeout=self.settings.request_timeout_seconds,
                )
                response_content_type = response.headers.get(
                    "Content-Type",
                    "application/octet-stream",
                )
                response_metadata = self.writer.write(
                    response_key,
                    response.content,
                    content_type=response_content_type,
                )
                self.state.record_file(
                    file_id=active_file_id,
                    metadata=response_metadata,
                    http_status=response.status_code,
                    request_attempt_count=response_attempt_count(response),
                )
                response.raise_for_status()
                if "json" not in response_content_type.lower():
                    raise ValueError("Open-Meteo response Content-Type is not JSON")
                received_count = received_location_count(
                    response.content,
                    len(task.locations),
                )
                self.state.validate_file(
                    active_file_id,
                    received_item_count=received_count,
                )
                response_objects.append(response_metadata)
                active_file_id = None

            self.state.succeed_run(
                attempt.attempt_id,
                completed_at_utc=self.clock(),
            )
            committed_attempt = RunAttempt(
                attempt_id=attempt.attempt_id,
                logical_run_id=attempt.logical_run_id,
                attempt_number=attempt.attempt_number,
                logical_key=attempt.logical_key,
                status=RunStatus.SUCCEEDED,
            )
            return ArchiveCollectionResult(
                attempt=committed_attempt,
                year_prefix=layout.year_prefix(year_plan.year),
                response_objects=tuple(response_objects),
            )
        except Exception as error:
            if active_file_id is not None:
                try:
                    self.state.fail_file(active_file_id, error=error)
                except Exception as state_error:  # noqa: BLE001  # pragma: no cover
                    error.add_note(f"Could not mark source file failed: {state_error}")
            try:
                self.state.fail_run(
                    attempt.attempt_id,
                    failed_at_utc=self.clock(),
                    error=error,
                )
            except Exception as state_error:  # noqa: BLE001  # pragma: no cover
                error.add_note(
                    f"Could not mark ingestion attempt failed: {state_error}"
                )
            raise


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


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("scheduled-at must include a UTC offset")
    return parsed.astimezone(UTC)


def _source_period(year: int, month: int | None, available_through: date) -> tuple[date, date]:
    if year > available_through.year:
        raise ValueError(f"year {year} is later than available data")
    if month is None:
        start_date = date(year, 1, 1)
        end_date = min(date(year, 12, 31), available_through)
    else:
        start_date = date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        end_date = min(date(year, month, last_day), available_through)
    if end_date < start_date:
        raise ValueError(f"source period starts after available date {available_through}")
    return start_date, end_date


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=_positive_int, required=True)
    parser.add_argument("--month", type=_month)
    parser.add_argument("--limit", type=_positive_int)
    parser.add_argument("--scheduled-at", type=_parse_utc)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Call Archive API for this one period; default only prints the plan.",
    )
    args = parser.parse_args()
    if args.execute and args.month is None:
        parser.error(
            "full-year source runs are disabled because a late HTTP failure would "
            "replay completed months; use run-open-meteo-archive --year YYYY "
            "--execute for monthly checkpoints"
        )
    settings = load_settings()
    observed_at_utc = datetime.now(UTC)
    available_through = latest_complete_archive_date(observed_at_utc.date())
    try:
        start_date, end_date = _source_period(
            args.year,
            args.month,
            available_through,
        )
    except ValueError as error:
        parser.error(str(error))

    connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_hanoi_locations(connection)
    finally:
        connection.close()
    if args.limit is not None:
        locations = locations[: args.limit]
    scope = f"canary_{len(locations)}" if args.limit is not None else "backfill"
    plan = plan_archive_year(
        locations,
        start_date=start_date,
        end_date=end_date,
        model=settings.open_meteo.archive_model,
        location_batch_size=settings.open_meteo.location_batch_size,
    )
    reserved_units = effective_call_units(
        plan.tasks,
        request_attempts=settings.open_meteo.max_attempts,
    )
    print(
        "Archive collection plan: "
        f"logical_key={plan.logical_key}, scope={scope}, locations={len(locations)}, "
        f"files={plan.expected_file_count}, expected_rows={plan.expected_row_count}, "
        f"worst_case_call_reserve={reserved_units}"
    )
    if not args.execute:
        print("DRY RUN: pass --execute to collect this single period")
        return

    minio = get_minio_client(settings.minio)
    ensure_bucket(minio, settings.minio.bucket)
    control_connection = connect_control_plane(settings.postgres)
    try:
        ensure_ingestion_state(control_connection)
        with build_http_session(
            max_attempts=settings.open_meteo.max_attempts
        ) as session:
            try:
                result = ArchiveCollector(
                    settings=settings.open_meteo,
                    session=session,
                    writer=ImmutableObjectWriter(minio, settings.minio.bucket),
                    state=PostgresIngestionRepository(control_connection),
                ).collect_year(
                    plan,
                    scheduled_at_utc=args.scheduled_at or observed_at_utc,
                    scope=scope,
                )
            except RunAlreadySucceededError:
                print(
                    "Archive source collection already succeeded: "
                    f"logical_key={plan.logical_key}, scope={scope}"
                )
                return
    finally:
        control_connection.close()
    print(
        "Archive source collection succeeded: "
        f"attempt_id={result.attempt.attempt_id} prefix={result.year_prefix}, "
        f"files={len(result.response_objects)}"
    )


if __name__ == "__main__":
    main()
