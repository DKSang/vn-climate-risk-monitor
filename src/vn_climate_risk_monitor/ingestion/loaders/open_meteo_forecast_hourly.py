"""Load verified Open-Meteo response files into the Bronze hourly table."""

from __future__ import annotations

import argparse
import os
import re
import socket
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

import duckdb

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.open_meteo import (
    PARSER_VERSION,
    ForecastParseResult,
    parse_forecast_hourly,
)
from vn_climate_risk_monitor.ingestion.state import (
    ClaimedFile,
    PostgresIngestionRepository,
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import VerifiedObjectReader, get_minio_client

PIPELINE_NAME = "open_meteo_forecast"
DATASET = "forecast"
TARGET_TABLE = "bronze_store.tables.open_meteo_forecast_hourly"
STAGING_VIEW = "open_meteo_forecast_hourly_staging"
QUALIFIED_NAME = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*){0,2}$")

TARGET_DDL = """
CREATE TABLE IF NOT EXISTS {target_table} (
    bronze_row_id VARCHAR NOT NULL,
    attempt_id VARCHAR NOT NULL,
    logical_run_id VARCHAR NOT NULL,
    file_id VARCHAR NOT NULL,
    batch_index INTEGER NOT NULL,
    request_location_index INTEGER NOT NULL,
    source_location_id INTEGER,
    hourly_index INTEGER NOT NULL,
    ward_key BIGINT NOT NULL,
    scheduled_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    collection_started_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    collection_completed_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_time_utc TIMESTAMP WITH TIME ZONE,
    interval_start_utc TIMESTAMP WITH TIME ZONE,
    interval_end_utc TIMESTAMP WITH TIME ZONE,
    grid_latitude DOUBLE,
    grid_longitude DOUBLE,
    grid_elevation DOUBLE,
    generationtime_ms DOUBLE,
    utc_offset_seconds INTEGER,
    timezone VARCHAR,
    timezone_abbreviation VARCHAR,
    source_endpoint VARCHAR NOT NULL,
    model_requested VARCHAR NOT NULL,
    forecast_hours INTEGER NOT NULL,
    precipitation DOUBLE,
    rain DOUBLE,
    showers DOUBLE,
    precipitation_probability DOUBLE,
    weather_code INTEGER,
    hourly_units_json VARCHAR NOT NULL,
    _source_file_path VARCHAR NOT NULL,
    _source_file_sha256 VARCHAR NOT NULL,
    _collector_version VARCHAR NOT NULL,
    _request_contract_version INTEGER NOT NULL,
    _parser_version VARCHAR NOT NULL,
    _rescued_data VARCHAR,
    _ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL
)
"""


class SourceReader(Protocol):
    def read(
        self,
        object_key: str,
        *,
        expected_size: int,
        expected_sha256: str,
    ) -> bytes: ...


class LoaderState(Protocol):
    def claim_files(self, **values: object) -> tuple[ClaimedFile, ...]: ...

    def commit_file(
        self,
        file_id: UUID,
        *,
        worker_id: str,
        committed_at_utc: datetime,
        rows_parsed: int,
        rows_inserted: int,
        rescued_rows: int,
        parser_version: str,
    ) -> None: ...

    def fail_file(
        self,
        file_id: UUID,
        *,
        error: BaseException,
        worker_id: str | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class MergeResult:
    rows_parsed: int
    rows_inserted: int


@dataclass(frozen=True)
class LoadFailure:
    file_id: UUID
    object_key: str
    error_type: str
    error_message: str


@dataclass(frozen=True)
class LoadSummary:
    claimed_files: int
    committed_files: int
    rows_parsed: int
    rows_inserted: int
    rescued_rows: int
    failures: tuple[LoadFailure, ...]


def _validated_name(value: str) -> str:
    if not QUALIFIED_NAME.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def ensure_forecast_hourly_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    target_table: str = TARGET_TABLE,
) -> None:
    """Create the explicit Bronze table without unsupported constraints."""
    target_table = _validated_name(target_table)
    connection.execute(TARGET_DDL.format(target_table=target_table))
    connection.execute(
        f"ALTER TABLE {target_table} "
        "ADD COLUMN IF NOT EXISTS source_location_id INTEGER"
    )


def merge_forecast_hourly(
    connection: duckdb.DuckDBPyConnection,
    parsed: ForecastParseResult,
    *,
    target_table: str = TARGET_TABLE,
    staging_view: str = STAGING_VIEW,
) -> MergeResult:
    """Merge one parsed file atomically using its deterministic row IDs."""
    if parsed.row_count < 1:
        raise ValueError("Cannot merge an empty Arrow table")
    target_table = _validated_name(target_table)
    staging_view = _validated_name(staging_view)
    connection.register(staging_view, parsed.table)
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            ensure_forecast_hourly_table(connection, target_table=target_table)
            matched = connection.execute(
                f"""
                SELECT count(*)
                FROM {target_table} AS target
                JOIN {staging_view} AS source USING (bronze_row_id)
                """
            ).fetchone()[0]
            connection.execute(
                f"""
                MERGE INTO {target_table} AS target
                USING {staging_view} AS source
                ON target.bronze_row_id = source.bronze_row_id
                WHEN NOT MATCHED THEN INSERT BY NAME
                """
            )
            source_file_id = parsed.table.column("file_id")[0].as_py()
            target_rows = connection.execute(
                f"SELECT count(*) FROM {target_table} WHERE file_id = ?",
                (source_file_id,),
            ).fetchone()[0]
            if target_rows != parsed.row_count:
                raise RuntimeError(
                    "Bronze verification failed: "
                    f"expected {parsed.row_count} rows for file, found {target_rows}"
                )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.unregister(staging_view)
    return MergeResult(
        rows_parsed=parsed.row_count,
        rows_inserted=parsed.row_count - matched,
    )


class ForecastHourlyLoader:
    """Available-now loader that keeps PostgreSQL and Bronze commits ordered."""

    def __init__(
        self,
        *,
        state: LoaderState,
        reader: SourceReader,
        bronze_connection: duckdb.DuckDBPyConnection,
        target_table: str = TARGET_TABLE,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.state = state
        self.reader = reader
        self.bronze_connection = bronze_connection
        self.target_table = _validated_name(target_table)
        self.clock = clock or (lambda: datetime.now(UTC))

    def load_available(
        self,
        *,
        scope: str,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        max_retries: int,
    ) -> LoadSummary:
        if not scope.strip() or not worker_id.strip():
            raise ValueError("scope and worker_id must not be empty")
        claimed = self.state.claim_files(
            pipeline_name=PIPELINE_NAME,
            dataset=DATASET,
            scope=scope,
            worker_id=worker_id,
            limit=limit,
            lease_seconds=lease_seconds,
            max_retries=max_retries,
        )
        committed_files = 0
        rows_parsed = 0
        rows_inserted = 0
        rescued_rows = 0
        failures: list[LoadFailure] = []
        for source in claimed:
            try:
                content = self.reader.read(
                    source.object_key,
                    expected_size=source.size_bytes,
                    expected_sha256=source.sha256,
                )
                parsed = parse_forecast_hourly(
                    content,
                    source=source,
                    ingested_at_utc=self.clock(),
                )
                merged = merge_forecast_hourly(
                    self.bronze_connection,
                    parsed,
                    target_table=self.target_table,
                )
                self.state.commit_file(
                    source.file_id,
                    worker_id=worker_id,
                    committed_at_utc=self.clock(),
                    rows_parsed=merged.rows_parsed,
                    rows_inserted=merged.rows_inserted,
                    rescued_rows=parsed.rescued_row_count,
                    parser_version=PARSER_VERSION,
                )
                committed_files += 1
                rows_parsed += merged.rows_parsed
                rows_inserted += merged.rows_inserted
                rescued_rows += parsed.rescued_row_count
            except Exception as error:  # noqa: BLE001
                try:
                    self.state.fail_file(
                        source.file_id,
                        worker_id=worker_id,
                        error=error,
                    )
                except Exception as state_error:  # noqa: BLE001  # pragma: no cover
                    error.add_note(f"Could not mark claimed file failed: {state_error}")
                failures.append(
                    LoadFailure(
                        file_id=source.file_id,
                        object_key=source.object_key,
                        error_type=type(error).__name__,
                        error_message=str(error),
                    )
                )
        return LoadSummary(
            claimed_files=len(claimed),
            committed_files=committed_files,
            rows_parsed=rows_parsed,
            rows_inserted=rows_inserted,
            rescued_rows=rescued_rows,
            failures=tuple(failures),
        )


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must not be negative")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", default="production")
    parser.add_argument("--limit", type=_positive_int)
    parser.add_argument("--lease-seconds", type=_positive_int)
    parser.add_argument("--max-retries", type=_nonnegative_int)
    parser.add_argument("--worker-id")
    args = parser.parse_args()
    settings = load_settings()
    worker_id = args.worker_id or f"{socket.gethostname()}-{os.getpid()}"
    limit = args.limit or settings.open_meteo.loader_batch_size
    lease_seconds = args.lease_seconds or settings.open_meteo.loader_lease_seconds
    max_retries = (
        args.max_retries
        if args.max_retries is not None
        else settings.open_meteo.loader_max_retries
    )

    control_connection = connect_control_plane(settings.postgres)
    bronze_connection = get_connection()
    try:
        ensure_ingestion_state(control_connection)
        state = PostgresIngestionRepository(control_connection)
        reader = VerifiedObjectReader(
            get_minio_client(settings.minio),
            settings.minio.bucket,
        )
        summary = ForecastHourlyLoader(
            state=state,
            reader=reader,
            bronze_connection=bronze_connection,
        ).load_available(
            scope=args.scope,
            worker_id=worker_id,
            limit=limit,
            lease_seconds=lease_seconds,
            max_retries=max_retries,
        )
    finally:
        bronze_connection.close()
        control_connection.close()

    print(
        f"Forecast Bronze load: claimed={summary.claimed_files}, "
        f"committed={summary.committed_files}, rows={summary.rows_parsed}, "
        f"inserted={summary.rows_inserted}, rescued={summary.rescued_rows}, "
        f"failed={len(summary.failures)}"
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
