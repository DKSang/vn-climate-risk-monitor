import hashlib
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
import responses

from vn_climate_risk_monitor.config import OpenMeteoSettings
from vn_climate_risk_monitor.ingestion.collectors.open_meteo_archive import (
    ArchiveCollector,
    effective_call_units,
)
from vn_climate_risk_monitor.ingestion.http import build_http_session
from vn_climate_risk_monitor.ingestion.open_meteo import (
    RequestedLocation,
    SourceObjectMetadata,
    plan_archive_backfill,
)
from vn_climate_risk_monitor.ingestion.state import RunAttempt, RunStatus

ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000101")
LOGICAL_RUN_ID = UUID("00000000-0000-0000-0000-000000000102")


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


class MemoryPacer:
    def __init__(self) -> None:
        self.call_units: list[int] = []

    def wait(self, call_units: int) -> float:
        self.call_units.append(call_units)
        return 0


class MemoryIngestionState:
    def __init__(self) -> None:
        self.run_values: dict[str, object] = {}
        self.run_status = RunStatus.RUNNING
        self.files: dict[UUID, dict[str, object]] = {}
        self.recorded_attempts = 0

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
        assert pipeline_name == "open_meteo_archive"
        assert since_utc.tzinfo is not None
        return self.recorded_attempts

    def register_file(
        self,
        *,
        attempt_id: UUID,
        batch_index: int,
        object_key: str,
        expected_item_count: int,
        file_parameters: dict[str, object],
    ) -> UUID:
        assert attempt_id == ATTEMPT_ID
        file_id = UUID(int=batch_index + 200)
        self.files[file_id] = {
            "status": "PENDING",
            "object_key": object_key,
            "expected_item_count": expected_item_count,
            "file_parameters": file_parameters,
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

    def validate_file(self, file_id: UUID, *, received_item_count: int) -> None:
        self.files[file_id]["received_item_count"] = received_item_count

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


def _settings() -> OpenMeteoSettings:
    return OpenMeteoSettings(
        forecast_url="https://api.open-meteo.test/v1/forecast",
        archive_url="https://archive.open-meteo.test/v1/archive",
        forecast_model="best_match",
        archive_model="era5",
        forecast_hours=72,
        location_batch_size=2,
        concurrency=1,
        request_timeout_seconds=10,
        max_attempts=1,
        max_effective_calls_per_minute=1_000_000_000,
        max_effective_calls_per_hour=1_000_000_000,
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


def _year_plan(
    *,
    location_count: int = 3,
    end_date: date = date(2000, 1, 31),
    hourly_variables: tuple[str, ...] | None = None,
):
    values: dict[str, object] = {}
    if hourly_variables is not None:
        values["hourly_variables"] = hourly_variables
    return plan_archive_backfill(
        _locations(location_count),
        start_year=2000,
        end_date=end_date,
        model="era5",
        location_batch_size=2,
        **values,
    ).years[0]


def _clock(start: datetime):
    current = start

    def tick() -> datetime:
        nonlocal current
        value = current
        current += timedelta(seconds=1)
        return value

    return tick


@responses.activate
def test_archive_collector_writes_exact_monthly_responses_and_generic_state() -> None:
    bodies = (b'[{"latitude":21.1},{"latitude":21.2}]', b'{"latitude":21.3}')
    for body in bodies:
        responses.add(
            responses.GET,
            "https://archive.open-meteo.test/v1/archive",
            body=body,
            status=200,
            content_type="application/json",
        )
    writer = MemoryObjectWriter()
    state = MemoryIngestionState()
    pacer = MemoryPacer()
    scheduled_at = datetime(2026, 8, 21, 12, tzinfo=UTC)

    with build_http_session(max_attempts=1) as session:
        result = ArchiveCollector(
            settings=_settings(),
            session=session,
            writer=writer,
            state=state,
            pacer=pacer,
            clock=_clock(scheduled_at + timedelta(seconds=1)),
        ).collect_year(_year_plan(), scheduled_at_utc=scheduled_at)

    assert state.run_status == RunStatus.SUCCEEDED
    assert state.run_values["pipeline_name"] == "open_meteo_archive"
    assert state.run_values["dataset"] == "historical_weather_hourly"
    assert state.run_values["logical_key"] == (
        "model=era5/year=2000/through=2000-01-31"
    )
    assert state.run_values["run_parameters"]["model"] == "era5"
    assert result.year_prefix.endswith("backfill/year=2000")
    assert len(result.response_objects) == 2
    assert writer.objects[writer.write_order[0]] == bodies[0]
    assert writer.objects[writer.write_order[1]] == bodies[1]
    assert all("year=2000/month=01" in key for key in writer.write_order)
    assert [
        file["file_parameters"]["ward_keys"] for file in state.files.values()
    ] == [[1, 2], [3]]
    assert [
        file["received_item_count"] for file in state.files.values()
    ] == [2, 1]
    assert responses.calls[0].request.params["models"] == "era5"
    assert responses.calls[0].request.params["start_date"] == "2000-01-01"
    assert responses.calls[0].request.params["end_date"] == "2000-01-31"
    assert pacer.call_units == [5, 3]


@responses.activate
def test_archive_collector_keeps_error_body_and_fails_file_and_run() -> None:
    body = b'{"error":true,"reason":"invalid archive request"}'
    responses.add(
        responses.GET,
        "https://archive.open-meteo.test/v1/archive",
        body=body,
        status=400,
        content_type="application/json",
    )
    writer = MemoryObjectWriter()
    state = MemoryIngestionState()
    scheduled_at = datetime(2026, 8, 21, 12, tzinfo=UTC)

    with build_http_session(max_attempts=1) as session:
        collector = ArchiveCollector(
            settings=_settings(),
            session=session,
            writer=writer,
            state=state,
            clock=_clock(scheduled_at + timedelta(seconds=1)),
        )
        with pytest.raises(Exception, match="400"):
            collector.collect_year(
                _year_plan(location_count=1),
                scheduled_at_utc=scheduled_at,
            )

    assert writer.objects[writer.write_order[0]] == body
    assert state.run_status == RunStatus.FAILED
    file = next(iter(state.files.values()))
    assert file["http_status"] == 400
    assert file["status"] == "FAILED"


def test_effective_call_estimate_matches_documented_time_and_variable_factors() -> None:
    variables = tuple(f"variable_{index}" for index in range(15))
    task = _year_plan(
        location_count=1,
        end_date=date(2000, 1, 28),
        hourly_variables=variables,
    ).tasks[0]

    assert effective_call_units((task,)) == 3


def test_effective_call_estimate_multiplies_multi_location_requests() -> None:
    tasks = _year_plan(
        location_count=3,
        end_date=date(2000, 1, 14),
    ).tasks

    assert effective_call_units(tasks, request_attempts=5) == 15
