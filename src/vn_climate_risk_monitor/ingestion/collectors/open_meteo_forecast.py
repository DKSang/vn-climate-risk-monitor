"""Collect immutable Open-Meteo forecast responses into Bronze files."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

import duckdb
from requests import Response, Session

from vn_climate_risk_monitor.config import OpenMeteoSettings, load_settings
from vn_climate_risk_monitor.ingestion.http import build_http_session
from vn_climate_risk_monitor.ingestion.layout import BronzeFilesLayout
from vn_climate_risk_monitor.ingestion.open_meteo import (
    COLLECTOR_VERSION,
    FORECAST_HOURLY_VARIABLES,
    REQUEST_CONTRACT_VERSION,
    ForecastRequestContract,
    RequestedLocation,
    SourceObjectMetadata,
    split_location_batches,
)
from vn_climate_risk_monitor.ingestion.scheduling import latest_hourly_schedule_slot
from vn_climate_risk_monitor.ingestion.state import (
    PostgresIngestionRepository,
    RunAttempt,
    RunStatus,
    connect_control_plane,
    ensure_ingestion_state,
    logical_schedule_key,
)
from vn_climate_risk_monitor.lakehouse import get_connection
from vn_climate_risk_monitor.storage import (
    ImmutableObjectWriter,
    ensure_bucket,
    get_minio_client,
)

PIPELINE_NAME = "open_meteo_forecast"
DATASET = "forecast"
EXPECTED_HANOI_LOCATION_COUNT = 126


class ObjectWriter(Protocol):
    """Minimal write-once storage boundary required by the collector."""

    def write(
        self,
        object_key: str,
        content: bytes,
        *,
        content_type: str,
    ) -> SourceObjectMetadata: ...


class IngestionState(Protocol):
    """Control-plane operations required by the collector."""

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
        ward_keys: Sequence[int],
    ) -> UUID: ...

    def record_file(
        self,
        *,
        file_id: UUID,
        metadata: SourceObjectMetadata,
        http_status: int,
        request_attempt_count: int,
    ) -> None: ...

    def validate_file(self, file_id: UUID, *, received_location_count: int) -> None: ...

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
class ForecastCollectionResult:
    attempt: RunAttempt
    run_prefix: str
    response_objects: tuple[SourceObjectMetadata, ...]


def load_hanoi_locations(
    connection: duckdb.DuckDBPyConnection,
    *,
    expected_count: int = EXPECTED_HANOI_LOCATION_COUNT,
) -> tuple[RequestedLocation, ...]:
    """Read the approved Gold dimension in deterministic business-key order."""
    rows = connection.execute(
        """
        SELECT ward_key, ward_code, latitude, longitude
        FROM gold.dim_hanoi_ward
        ORDER BY ward_key
        """
    ).fetchall()
    locations = tuple(
        RequestedLocation(
            ward_key=int(ward_key),
            ward_code=str(ward_code),
            latitude=float(latitude),
            longitude=float(longitude),
        )
        for ward_key, ward_code, latitude, longitude in rows
    )
    if len(locations) != expected_count:
        raise ValueError(
            f"Expected {expected_count} Hanoi locations, received {len(locations)}"
        )
    split_location_batches(locations, batch_size=max(1, len(locations)))
    return locations


def _response_attempt_count(response: Response) -> int:
    retries = getattr(getattr(response, "raw", None), "retries", None)
    history = getattr(retries, "history", ())
    return 1 + len(history)


def _received_location_count(content: bytes, expected_count: int) -> int:
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Open-Meteo response is not valid JSON") from error

    if isinstance(payload, dict):
        if payload.get("error") is True:
            reason = payload.get("reason", "unknown API error")
            raise ValueError(f"Open-Meteo returned an error payload: {reason}")
        received_count = 1
    elif isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError("Open-Meteo response list must contain only objects")
        received_count = len(payload)
    else:
        raise TypeError("Open-Meteo response root must be an object or array")

    if received_count != expected_count:
        raise ValueError(
            f"Expected {expected_count} response locations, received {received_count}"
        )
    return received_count


class ForecastCollector:
    """Sequential collector with PostgreSQL as the metadata source of truth."""

    def __init__(
        self,
        *,
        settings: OpenMeteoSettings,
        session: Session,
        writer: ObjectWriter,
        state: IngestionState,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if settings.concurrency != 1:
            raise ValueError("Phase 2 collector supports concurrency=1 only")
        self.settings = settings
        self.session = session
        self.writer = writer
        self.state = state
        self.clock = clock or (lambda: datetime.now(UTC))

    def collect(
        self,
        locations: Sequence[RequestedLocation],
        *,
        scheduled_at_utc: datetime,
        scope: str = "production",
    ) -> ForecastCollectionResult:
        if scheduled_at_utc.tzinfo is None or scheduled_at_utc.utcoffset() is None:
            raise ValueError("scheduled_at_utc must be timezone-aware")
        if not scope.strip():
            raise ValueError("scope must not be empty")
        scheduled_at_utc = scheduled_at_utc.astimezone(UTC)
        batches = split_location_batches(locations, self.settings.location_batch_size)
        started_at_utc = self.clock()
        if started_at_utc.tzinfo is None or started_at_utc.utcoffset() is None:
            raise ValueError("collector clock must return timezone-aware timestamps")
        if started_at_utc < scheduled_at_utc:
            raise ValueError("collector cannot start before scheduled_at_utc")

        attempt = self.state.start_run(
            pipeline_name=PIPELINE_NAME,
            dataset=DATASET,
            scope=scope,
            logical_key=logical_schedule_key(scheduled_at_utc),
            scheduled_at_utc=scheduled_at_utc,
            started_at_utc=started_at_utc,
            batch_count=len(batches),
            location_count=len(locations),
            source_endpoint=self.settings.forecast_url,
            model_requested=self.settings.forecast_model,
            forecast_hours=self.settings.forecast_hours,
            hourly_variables=FORECAST_HOURLY_VARIABLES,
            collector_version=COLLECTOR_VERSION,
            request_contract_version=REQUEST_CONTRACT_VERSION,
            stale_after_seconds=self.settings.collector_stale_after_seconds,
        )
        run_name = (
            f"forecast_{scheduled_at_utc:%Y%m%dt%H%M%Sz}_"
            f"a{attempt.attempt_number}_{attempt.attempt_id.hex[:8]}"
        )
        layout = BronzeFilesLayout("open_meteo", DATASET, "incremental")
        run_prefix = layout.run_prefix(started_at_utc, run_name)
        response_objects: list[SourceObjectMetadata] = []
        active_file_id: UUID | None = None

        try:
            budget_window_start = started_at_utc.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            used_calls = self.state.effective_call_count(
                pipeline_name=PIPELINE_NAME,
                since_utc=budget_window_start,
            )
            reserved_calls = len(batches) * self.settings.max_attempts
            if used_calls + reserved_calls > self.settings.max_effective_calls_per_day:
                raise RuntimeError(
                    "Open-Meteo daily call guardrail exceeded: "
                    f"used={used_calls}, requested_reserve={reserved_calls}, "
                    f"limit={self.settings.max_effective_calls_per_day}"
                )
            for batch_index, batch_locations in enumerate(batches):
                request = ForecastRequestContract(
                    endpoint=self.settings.forecast_url,
                    model=self.settings.forecast_model,
                    forecast_hours=self.settings.forecast_hours,
                    batch_index=batch_index,
                    scheduled_at_utc=scheduled_at_utc,
                    requested_at_utc=self.clock(),
                    locations=batch_locations,
                )
                response_key = layout.object_key(
                    started_at_utc,
                    run_name,
                    f"response_{batch_index:03}.json",
                )
                active_file_id = self.state.register_file(
                    attempt_id=attempt.attempt_id,
                    batch_index=batch_index,
                    object_key=response_key,
                    ward_keys=[location.ward_key for location in batch_locations],
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
                    request_attempt_count=_response_attempt_count(response),
                )
                response.raise_for_status()
                if "json" not in response_content_type.lower():
                    raise ValueError("Open-Meteo response Content-Type is not JSON")
                received_count = _received_location_count(
                    response.content,
                    len(batch_locations),
                )
                self.state.validate_file(
                    active_file_id,
                    received_location_count=received_count,
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
            return ForecastCollectionResult(
                attempt=committed_attempt,
                run_prefix=run_prefix,
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


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("scheduled-at must include a UTC offset")
    return parsed.astimezone(UTC)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Call Open-Meteo and persist responses; default only prints the plan.",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        help="Use the first N approved locations for an explicit canary.",
    )
    parser.add_argument(
        "--scheduled-at",
        type=parse_utc,
        help="Logical UTC schedule time; defaults to now.",
    )
    args = parser.parse_args()
    settings = load_settings()
    observed_at_utc = datetime.now(UTC)
    scheduled_at_utc = args.scheduled_at or latest_hourly_schedule_slot(
        observed_at_utc,
        minute=settings.open_meteo.schedule_minute_utc,
    )

    connection = get_connection(attach_bronze=False, read_only=True)
    try:
        locations = load_hanoi_locations(connection)
    finally:
        connection.close()
    if args.limit is not None:
        locations = locations[: args.limit]

    batch_count = len(
        split_location_batches(locations, settings.open_meteo.location_batch_size)
    )
    scope = f"canary_{len(locations)}" if args.limit is not None else "production"
    if not args.execute:
        print(
            f"DRY RUN: {len(locations)} locations, {batch_count} batches, "
            f"scope={scope}, model={settings.open_meteo.forecast_model}, "
            f"forecast_hours={settings.open_meteo.forecast_hours}"
        )
        return

    minio = get_minio_client(settings.minio)
    ensure_bucket(minio, settings.minio.bucket)
    writer = ImmutableObjectWriter(minio, settings.minio.bucket)
    control_connection = connect_control_plane(settings.postgres)
    try:
        ensure_ingestion_state(control_connection)
        state = PostgresIngestionRepository(control_connection)
        with build_http_session(
            max_attempts=settings.open_meteo.max_attempts
        ) as session:
            result = ForecastCollector(
                settings=settings.open_meteo,
                session=session,
                writer=writer,
                state=state,
            ).collect(
                locations,
                scheduled_at_utc=scheduled_at_utc,
                scope=scope,
            )
    finally:
        control_connection.close()
    print(
        "Forecast source collection succeeded: "
        f"attempt_id={result.attempt.attempt_id} prefix={result.run_prefix}"
    )


if __name__ == "__main__":
    main()
