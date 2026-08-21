import hashlib
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

import duckdb
import pytest
import responses

from vn_climate_risk_monitor.config import OpenMeteoSettings
from vn_climate_risk_monitor.ingestion.collectors.open_meteo_forecast import (
    ForecastCollector,
    load_hanoi_locations,
)
from vn_climate_risk_monitor.ingestion.http import build_http_session
from vn_climate_risk_monitor.ingestion.open_meteo import (
    RequestedLocation,
    SourceObjectMetadata,
)
from vn_climate_risk_monitor.ingestion.state import RunAttempt, RunStatus

ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000001")
LOGICAL_RUN_ID = UUID("00000000-0000-0000-0000-000000000002")


class MemoryObjectWriter:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.write_order: list[str] = []

    def write(
        self,
        object_key: str,
        content: bytes,
        *,
        content_type: str,
    ) -> SourceObjectMetadata:
        if object_key in self.objects:
            raise FileExistsError(object_key)
        self.objects[object_key] = content
        self.write_order.append(object_key)
        return SourceObjectMetadata(
            object_key=object_key,
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            content_type=content_type,
            etag=f"etag-{len(self.write_order)}",
        )


class MemoryIngestionState:
    def __init__(self) -> None:
        self.run_values: dict[str, object] = {}
        self.run_status = RunStatus.RUNNING
        self.files: dict[UUID, dict[str, object]] = {}
        self.effective_calls = 0

    def start_run(self, **values: object) -> RunAttempt:
        self.run_values = values
        return RunAttempt(
            attempt_id=ATTEMPT_ID,
            logical_run_id=LOGICAL_RUN_ID,
            attempt_number=1,
            logical_key=str(values["logical_key"]),
            status=RunStatus.RUNNING,
        )

    def effective_call_count(
        self,
        *,
        pipeline_name: str,
        since_utc: datetime,
    ) -> int:
        assert pipeline_name == "open_meteo_forecast"
        assert since_utc.tzinfo is not None
        return self.effective_calls

    def register_file(
        self,
        *,
        attempt_id: UUID,
        batch_index: int,
        object_key: str,
        ward_keys: Sequence[int],
    ) -> UUID:
        assert attempt_id == ATTEMPT_ID
        file_id = UUID(int=batch_index + 10)
        self.files[file_id] = {
            "status": "PENDING",
            "object_key": object_key,
            "ward_keys": tuple(ward_keys),
        }
        return file_id

    def record_file(
        self,
        *,
        file_id: UUID,
        metadata: SourceObjectMetadata,
        http_status: int,
        request_attempt_count: int,
    ) -> None:
        self.files[file_id].update(
            metadata=metadata,
            http_status=http_status,
            request_attempt_count=request_attempt_count,
        )

    def validate_file(self, file_id: UUID, *, received_location_count: int) -> None:
        self.files[file_id]["received_location_count"] = received_location_count

    def succeed_run(self, attempt_id: UUID, *, completed_at_utc: datetime) -> None:
        assert attempt_id == ATTEMPT_ID
        assert completed_at_utc.tzinfo is not None
        self.run_status = RunStatus.SUCCEEDED

    def fail_file(self, file_id: UUID, *, error: BaseException) -> None:
        self.files[file_id].update(status="FAILED", error=error)

    def fail_run(
        self,
        attempt_id: UUID,
        *,
        failed_at_utc: datetime,
        error: BaseException,
    ) -> None:
        assert attempt_id == ATTEMPT_ID
        assert failed_at_utc.tzinfo is not None
        self.run_status = RunStatus.FAILED
        self.run_values["error"] = error


def _settings(*, batch_size: int = 2) -> OpenMeteoSettings:
    return OpenMeteoSettings(
        forecast_url="https://api.open-meteo.test/v1/forecast",
        archive_url="https://archive.open-meteo.test/v1/archive",
        forecast_model="best_match",
        archive_model="era5_land",
        forecast_hours=72,
        location_batch_size=batch_size,
        concurrency=1,
        request_timeout_seconds=10,
        max_attempts=1,
        max_effective_calls_per_day=1000,
        schedule_minute_utc=15,
        collector_stale_after_seconds=1800,
        loader_batch_size=10,
        loader_lease_seconds=300,
        loader_max_retries=3,
        max_load_batches=100,
        stale_after_minutes=120,
    )


def _locations(count: int) -> tuple[RequestedLocation, ...]:
    return tuple(
        RequestedLocation(
            ward_key=index,
            ward_code=f"{index:05}",
            latitude=21 + index / 1000,
            longitude=105.8 + index / 1000,
        )
        for index in range(1, count + 1)
    )


def _clock(start: datetime, count: int = 20):
    values: Iterator[datetime] = iter(
        start + timedelta(seconds=i) for i in range(count)
    )
    return lambda: next(values)


@responses.activate
def test_forecast_collector_commits_only_response_objects_and_postgres_state() -> None:
    first_response = b'[{"latitude":21.1},{"latitude":21.2}]'
    second_response = b'{"latitude":21.3}'
    responses.add(
        responses.GET,
        "https://api.open-meteo.test/v1/forecast",
        body=first_response,
        status=200,
        content_type="application/json",
    )
    responses.add(
        responses.GET,
        "https://api.open-meteo.test/v1/forecast",
        body=second_response,
        status=200,
        content_type="application/json",
    )
    writer = MemoryObjectWriter()
    state = MemoryIngestionState()
    scheduled_at = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)

    with build_http_session(max_attempts=1) as session:
        result = ForecastCollector(
            settings=_settings(),
            session=session,
            writer=writer,
            state=state,
            clock=_clock(scheduled_at + timedelta(seconds=1)),
        ).collect(_locations(3), scheduled_at_utc=scheduled_at)

    basenames = [key.rsplit("/", 1)[-1] for key in writer.write_order]
    assert basenames == ["response_000.json", "response_001.json"]
    assert writer.objects[writer.write_order[0]] == first_response
    assert writer.objects[writer.write_order[1]] == second_response
    assert state.run_status == RunStatus.SUCCEEDED
    assert state.run_values["scope"] == "production"
    assert [file["ward_keys"] for file in state.files.values()] == [(1, 2), (3,)]
    assert [file["received_location_count"] for file in state.files.values()] == [2, 1]
    assert result.attempt.attempt_id == ATTEMPT_ID
    assert result.attempt.status == RunStatus.SUCCEEDED
    assert len(result.response_objects) == 2
    assert responses.calls[0].request.params["latitude"] == "21.001,21.002"
    assert state.run_values["stale_after_seconds"] == 1800


def test_forecast_collector_rejects_run_before_daily_call_budget_is_exceeded() -> None:
    writer = MemoryObjectWriter()
    state = MemoryIngestionState()
    state.effective_calls = 1000
    scheduled_at = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)

    with build_http_session(max_attempts=1) as session:
        collector = ForecastCollector(
            settings=_settings(batch_size=1),
            session=session,
            writer=writer,
            state=state,
            clock=_clock(scheduled_at + timedelta(seconds=1)),
        )
        with pytest.raises(RuntimeError, match="daily call guardrail exceeded"):
            collector.collect(_locations(1), scheduled_at_utc=scheduled_at)

    assert not writer.objects
    assert state.run_status == RunStatus.FAILED


@responses.activate
def test_forecast_collector_marks_partial_response_failed_but_keeps_raw_body() -> None:
    body = b'[{"latitude":21.1}]'
    responses.add(
        responses.GET,
        "https://api.open-meteo.test/v1/forecast",
        body=body,
        status=200,
        content_type="application/json",
    )
    writer = MemoryObjectWriter()
    state = MemoryIngestionState()
    scheduled_at = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)

    with build_http_session(max_attempts=1) as session:
        collector = ForecastCollector(
            settings=_settings(),
            session=session,
            writer=writer,
            state=state,
            clock=_clock(scheduled_at + timedelta(seconds=1)),
        )
        with pytest.raises(ValueError, match="Expected 2 response locations"):
            collector.collect(_locations(2), scheduled_at_utc=scheduled_at)

    assert [key.rsplit("/", 1)[-1] for key in writer.write_order] == [
        "response_000.json"
    ]
    assert writer.objects[writer.write_order[0]] == body
    assert state.run_status == RunStatus.FAILED
    assert next(iter(state.files.values()))["status"] == "FAILED"


@responses.activate
def test_forecast_collector_records_http_error_body_and_failed_state() -> None:
    body = b'{"error":true,"reason":"rate limited"}'
    responses.add(
        responses.GET,
        "https://api.open-meteo.test/v1/forecast",
        body=body,
        status=429,
        content_type="application/json",
    )
    writer = MemoryObjectWriter()
    state = MemoryIngestionState()
    scheduled_at = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)

    with build_http_session(max_attempts=1) as session:
        collector = ForecastCollector(
            settings=_settings(batch_size=1),
            session=session,
            writer=writer,
            state=state,
            clock=_clock(scheduled_at + timedelta(seconds=1)),
        )
        with pytest.raises(Exception, match="429"):
            collector.collect(_locations(1), scheduled_at_utc=scheduled_at)

    file_state = next(iter(state.files.values()))
    assert writer.objects[writer.write_order[0]] == body
    assert file_state["http_status"] == 429
    assert file_state["status"] == "FAILED"
    assert state.run_status == RunStatus.FAILED


def test_load_hanoi_locations_reads_approved_gold_grain() -> None:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA gold")
    connection.execute(
        """
        CREATE TABLE gold.dim_hanoi_ward (
            ward_key INTEGER,
            ward_code VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gold.dim_hanoi_ward VALUES
            (2, '00002', 21.02, 105.82),
            (1, '00001', 21.01, 105.81)
        """
    )

    locations = load_hanoi_locations(connection, expected_count=2)

    assert [location.ward_key for location in locations] == [1, 2]
    assert [location.ward_code for location in locations] == ["00001", "00002"]
