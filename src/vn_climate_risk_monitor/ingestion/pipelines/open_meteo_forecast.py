"""Run one scheduled Open-Meteo collection and drain its Bronze backlog."""

from __future__ import annotations

import argparse
import os
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.collectors.open_meteo_forecast import (
    DATASET,
    PIPELINE_NAME,
    ForecastCollectionResult,
    ForecastCollector,
    parse_utc,
    positive_int,
)
from vn_climate_risk_monitor.ingestion.http import build_http_session
from vn_climate_risk_monitor.ingestion.loaders.open_meteo_forecast_hourly import (
    ForecastHourlyLoader,
    LoadFailure,
    LoadSummary,
)
from vn_climate_risk_monitor.ingestion.open_meteo import (
    RequestedLocation,
    split_location_batches,
)
from vn_climate_risk_monitor.ingestion.open_meteo.locations import (
    load_hanoi_locations,
)
from vn_climate_risk_monitor.ingestion.scheduling import latest_hourly_schedule_slot
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
    def collect(
        self, locations: Sequence[RequestedLocation], **values: object
    ) -> ForecastCollectionResult: ...


class Loader(Protocol):
    def load_available(self, **values: object) -> LoadSummary: ...


@dataclass(frozen=True)
class PipelineRunSummary:
    scheduled_at_utc: datetime
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


class ForecastPipeline:
    """Compose the existing collector and available-now loader."""

    def __init__(self, *, collector: Collector, loader: Loader) -> None:
        self.collector = collector
        self.loader = loader

    def run(
        self,
        locations: Sequence[RequestedLocation],
        *,
        scheduled_at_utc: datetime,
        scope: str,
        worker_id: str,
        loader_batch_size: int,
        lease_seconds: int,
        max_retries: int,
        max_load_batches: int,
    ) -> PipelineRunSummary:
        if min(loader_batch_size, lease_seconds, max_load_batches) < 1:
            raise ValueError("Pipeline batch and lease limits must be positive")

        attempt_id: UUID | None = None
        try:
            collection = self.collector.collect(
                locations,
                scheduled_at_utc=scheduled_at_utc,
                scope=scope,
            )
            outcome = CollectionOutcome.SUCCEEDED
            attempt_id = collection.attempt.attempt_id
        except RunAlreadySucceededError:
            # Expected on scheduler retry: the source run is immutable and the
            # loader can safely continue from its file checkpoints.
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
                "Loader did not drain within OPEN_METEO_MAX_LOAD_BATCHES"
            )

        return PipelineRunSummary(
            scheduled_at_utc=scheduled_at_utc,
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Persist source data and Bronze rows; default only prints the plan.",
    )
    parser.add_argument("--limit", type=positive_int)
    parser.add_argument("--scheduled-at", type=parse_utc)
    args = parser.parse_args()
    settings = load_settings()
    observed_at_utc = datetime.now(UTC)
    scheduled_at_utc = args.scheduled_at or latest_hourly_schedule_slot(
        observed_at_utc,
        minute=settings.open_meteo.schedule_minute_utc,
    )

    location_connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_hanoi_locations(location_connection)
    finally:
        location_connection.close()
    if args.limit is not None:
        locations = locations[: args.limit]
    scope = f"canary_{len(locations)}" if args.limit is not None else "production"
    batch_count = len(
        split_location_batches(locations, settings.open_meteo.location_batch_size)
    )
    print(
        f"Forecast pipeline plan: scheduled_at={scheduled_at_utc.isoformat()}, "
        f"scope={scope}, locations={len(locations)}, source_batches={batch_count}"
    )
    if not args.execute:
        print("DRY RUN: pass --execute to collect and load")
        return

    minio = get_minio_client(settings.minio)
    ensure_bucket(minio, settings.minio.bucket)
    control_connection = connect_control_plane(settings.postgres)
    bronze_connection = get_connection()
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    try:
        ensure_ingestion_state(control_connection)
        state = PostgresIngestionRepository(control_connection)
        writer = ImmutableObjectWriter(minio, settings.minio.bucket)
        reader = VerifiedObjectReader(minio, settings.minio.bucket)
        with build_http_session(
            max_attempts=settings.open_meteo.max_attempts
        ) as session:
            summary = ForecastPipeline(
                collector=ForecastCollector(
                    settings=settings.open_meteo,
                    session=session,
                    writer=writer,
                    state=state,
                ),
                loader=ForecastHourlyLoader(
                    state=state,
                    reader=reader,
                    bronze_connection=bronze_connection,
                ),
            ).run(
                locations,
                scheduled_at_utc=scheduled_at_utc,
                scope=scope,
                worker_id=worker_id,
                loader_batch_size=settings.open_meteo.loader_batch_size,
                lease_seconds=settings.open_meteo.loader_lease_seconds,
                max_retries=settings.open_meteo.loader_max_retries,
                max_load_batches=settings.open_meteo.max_load_batches,
            )
        metrics = state.pipeline_metrics(
            pipeline_name=PIPELINE_NAME,
            dataset=DATASET,
            scope=scope,
            max_retries=settings.open_meteo.loader_max_retries,
            observed_at_utc=datetime.now(UTC),
        )
        health = metrics.health(
            stale_after=timedelta(minutes=settings.open_meteo.stale_after_minutes)
        )
    finally:
        bronze_connection.close()
        control_connection.close()

    print(
        "Forecast pipeline result: "
        f"collection={summary.collection_outcome}, load_batches={summary.load_batches}, "
        f"committed_files={summary.committed_files}, rows={summary.rows_parsed}, "
        f"inserted={summary.rows_inserted}, rescued={summary.rescued_rows}, "
        f"failed={len(summary.failures)}, health={health}"
    )
    for failure in summary.failures:
        print(
            f"FAILED file_id={failure.file_id} "
            f"{failure.error_type}: {failure.error_message}"
        )
    if summary.failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
