import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import duckdb

from vn_climate_risk_monitor.ingestion.loaders.open_meteo_archive_hourly import (
    ArchiveHourlyLoader,
    merge_archive_hourly,
)
from vn_climate_risk_monitor.ingestion.open_meteo import parse_archive_hourly
from vn_climate_risk_monitor.ingestion.state import ClaimedObject


def _source() -> ClaimedObject:
    timestamp = datetime(2026, 8, 21, 12, tzinfo=UTC)
    return ClaimedObject(
        file_id=UUID("00000000-0000-0000-0000-000000000301"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000302"),
        logical_run_id=UUID("00000000-0000-0000-0000-000000000303"),
        pipeline_name="open_meteo_archive",
        source_name="open_meteo",
        dataset="historical_weather_hourly",
        scope="canary_1",
        object_key="bronze/files/open_meteo/archive/year=2000/response.json",
        batch_index=0,
        size_bytes=1,
        sha256="a" * 64,
        content_type="application/json",
        retry_count=0,
        scheduled_at_utc=timestamp,
        collection_started_at_utc=timestamp,
        collection_completed_at_utc=timestamp,
        source_uri="https://archive-api.open-meteo.test/v1/archive",
        collector_version="0.2.0",
        contract_version="1",
        run_parameters={
            "year": 2000,
            "start_date": "2000-01-01",
            "end_date": "2000-01-01",
            "model": "era5",
            "hourly_variables": [
                "precipitation",
                "rain",
                "weather_code",
                "soil_moisture_0_to_7cm",
                "soil_moisture_7_to_28cm",
            ],
            "location_count": 1,
            "request_granularity": "month",
        },
        file_parameters={
            "year": 2000,
            "month": 1,
            "start_date": "2000-01-01",
            "end_date": "2000-01-01",
            "location_batch_index": 0,
            "ward_keys": [11],
        },
        expected_item_count=1,
        received_item_count=1,
    )


def _content() -> bytes:
    first = datetime(2000, 1, 1, tzinfo=UTC)
    times = [int((first + timedelta(hours=index)).timestamp()) for index in range(24)]
    return json.dumps(
        {
            "latitude": 21.0,
            "longitude": 105.75,
            "elevation": 17.0,
            "utc_offset_seconds": 0,
            "timezone": "GMT",
            "timezone_abbreviation": "GMT",
            "hourly_units": {"time": "unixtime"},
            "hourly": {
                "time": times,
                "precipitation": [0.1] * 24,
                "rain": [0.1] * 24,
                "weather_code": [51] * 24,
                "soil_moisture_0_to_7cm": [0.3] * 24,
                "soil_moisture_7_to_28cm": [0.35] * 24,
            },
        }
    ).encode()


class MemoryReader:
    def read(
        self, object_key: str, *, expected_size: int, expected_sha256: str
    ) -> bytes:
        return _content()


class InvalidReader:
    def read(
        self, object_key: str, *, expected_size: int, expected_sha256: str
    ) -> bytes:
        return b"not-json"


class MemoryState:
    def __init__(self, source: ClaimedObject) -> None:
        self.source = source
        self.committed: dict[str, object] | None = None
        self.failed = False

    def claim_files(self, **values: object) -> tuple[ClaimedObject, ...]:
        assert values["pipeline_name"] == "open_meteo_archive"
        assert values["dataset"] == "historical_weather_hourly"
        return (self.source,)

    def commit_file(self, file_id: UUID, **values: object) -> None:
        assert file_id == self.source.file_id
        self.committed = values

    def fail_file(self, file_id: UUID, **values: object) -> None:
        assert file_id == self.source.file_id
        self.failed = True


def _parsed(source: ClaimedObject | None = None):
    return parse_archive_hourly(
        _content(),
        source=source or _source(),
        ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
    )


def test_archive_merge_upserts_stable_business_key_across_attempts() -> None:
    connection = duckdb.connect()
    first = merge_archive_hourly(connection, _parsed(), target_table="bronze_hourly")
    retried_source = replace(
        _source(),
        file_id=UUID("00000000-0000-0000-0000-000000000401"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000402"),
    )
    second = merge_archive_hourly(
        connection,
        _parsed(retried_source),
        target_table="bronze_hourly",
    )

    assert first.rows_inserted == 24
    assert second.rows_inserted == 0
    assert connection.execute("SELECT count(*) FROM bronze_hourly").fetchone()[0] == 24
    assert connection.execute(
        "SELECT count(*) FROM bronze_hourly WHERE file_id = ?",
        (str(retried_source.file_id),),
    ).fetchone()[0] == 24


def test_archive_loader_commits_bronze_before_checkpoint() -> None:
    source = _source()
    state = MemoryState(source)
    connection = duckdb.connect()
    summary = ArchiveHourlyLoader(
        state=state,
        reader=MemoryReader(),
        bronze_connection=connection,
        target_table="bronze_hourly",
        clock=lambda: datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
    ).load_available(
        scope="canary_1",
        worker_id="worker-1",
        limit=1,
        lease_seconds=300,
        max_retries=3,
    )

    assert summary.committed_files == 1
    assert summary.rows_parsed == 24
    assert summary.rows_inserted == 24
    assert summary.rescued_rows == 0
    assert state.committed is not None
    assert state.committed["parser_version"] == "0.1.0"
    assert not state.failed


def test_archive_loader_marks_parser_failure_in_control_state() -> None:
    state = MemoryState(_source())
    summary = ArchiveHourlyLoader(
        state=state,
        reader=InvalidReader(),
        bronze_connection=duckdb.connect(),
        target_table="bronze_hourly",
        clock=lambda: datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
    ).load_available(
        scope="canary_1",
        worker_id="worker-1",
        limit=1,
        lease_seconds=300,
        max_retries=3,
    )

    assert summary.committed_files == 0
    assert summary.failures[0].error_type == "ArchiveParseError"
    assert state.failed
    assert state.committed is None
