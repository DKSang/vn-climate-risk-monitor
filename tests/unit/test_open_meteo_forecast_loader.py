import json
from datetime import UTC, datetime
from uuid import UUID

import duckdb

from vn_climate_risk_monitor.ingestion.loaders.open_meteo_forecast_hourly import (
    ForecastHourlyLoader,
    merge_forecast_hourly,
)
from vn_climate_risk_monitor.ingestion.open_meteo import parse_forecast_hourly
from vn_climate_risk_monitor.ingestion.state import ClaimedFile


def _source() -> ClaimedFile:
    timestamp = datetime(2026, 8, 21, 10, 15, tzinfo=UTC)
    return ClaimedFile(
        file_id=UUID("00000000-0000-0000-0000-000000000001"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000002"),
        logical_run_id=UUID("00000000-0000-0000-0000-000000000003"),
        object_key="bronze/files/open_meteo/forecast/response_000.json",
        batch_index=0,
        ward_keys=(11,),
        size_bytes=1,
        sha256="a" * 64,
        content_type="application/json",
        retry_count=0,
        scheduled_at_utc=timestamp,
        collection_started_at_utc=timestamp,
        collection_completed_at_utc=timestamp,
        source_endpoint="https://api.open-meteo.test/v1/forecast",
        model_requested="best_match",
        forecast_hours=2,
        hourly_variables=("precipitation",),
        collector_version="0.2.0",
        request_contract_version=1,
        expected_location_count=1,
        received_location_count=1,
    )


def _content() -> bytes:
    return json.dumps(
        {
            "latitude": 21.1,
            "longitude": 105.8,
            "hourly_units": {"time": "unixtime", "precipitation": "mm"},
            "hourly": {
                "time": [1_787_306_400, 1_787_310_000],
                "precipitation": [1.2, 2.3],
                "rain": [1.0, 2.0],
                "showers": [0.2, 0.3],
                "precipitation_probability": [80, 90],
                "weather_code": [61, 63],
            },
        }
    ).encode()


class MemoryReader:
    def read(
        self, object_key: str, *, expected_size: int, expected_sha256: str
    ) -> bytes:
        assert object_key == _source().object_key
        return _content()


class InvalidJsonReader:
    def read(
        self, object_key: str, *, expected_size: int, expected_sha256: str
    ) -> bytes:
        return b"not-json"


class MemoryState:
    def __init__(self, source: ClaimedFile) -> None:
        self.source = source
        self.committed: dict[str, object] | None = None
        self.failed = False

    def claim_files(self, **values: object) -> tuple[ClaimedFile, ...]:
        assert values["scope"] == "canary_1"
        return (self.source,)

    def commit_file(self, file_id: UUID, **values: object) -> None:
        assert file_id == self.source.file_id
        self.committed = values

    def fail_file(self, file_id: UUID, **values: object) -> None:
        self.failed = True


def test_merge_is_idempotent_by_bronze_row_id() -> None:
    parsed = parse_forecast_hourly(
        _content(),
        source=_source(),
        ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
    )
    connection = duckdb.connect()

    first = merge_forecast_hourly(connection, parsed, target_table="bronze_hourly")
    second = merge_forecast_hourly(connection, parsed, target_table="bronze_hourly")

    assert first.rows_parsed == 2
    assert first.rows_inserted == 2
    assert second.rows_inserted == 0
    assert connection.execute("SELECT count(*) FROM bronze_hourly").fetchone()[0] == 2


def test_loader_orders_bronze_commit_before_file_checkpoint() -> None:
    source = _source()
    state = MemoryState(source)
    connection = duckdb.connect()
    loader = ForecastHourlyLoader(
        state=state,
        reader=MemoryReader(),
        bronze_connection=connection,
        target_table="bronze_hourly",
        clock=lambda: datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
    )

    summary = loader.load_available(
        scope="canary_1",
        worker_id="worker-1",
        limit=1,
        lease_seconds=300,
        max_retries=3,
    )

    assert summary.committed_files == 1
    assert summary.rows_parsed == 2
    assert summary.rows_inserted == 2
    assert not summary.failures
    assert state.committed is not None
    assert state.committed["rows_parsed"] == 2
    assert connection.execute("SELECT count(*) FROM bronze_hourly").fetchone()[0] == 2
    assert not state.failed


def test_loader_marks_claimed_file_failed_when_parser_rejects_source() -> None:
    source = _source()
    state = MemoryState(source)
    connection = duckdb.connect()
    loader = ForecastHourlyLoader(
        state=state,
        reader=InvalidJsonReader(),
        bronze_connection=connection,
        target_table="bronze_hourly",
        clock=lambda: datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
    )

    summary = loader.load_available(
        scope="canary_1",
        worker_id="worker-1",
        limit=1,
        lease_seconds=300,
        max_retries=3,
    )

    assert summary.committed_files == 0
    assert len(summary.failures) == 1
    assert summary.failures[0].error_type == "ForecastParseError"
    assert state.failed
    assert state.committed is None
