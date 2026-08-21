import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest

from vn_climate_risk_monitor.ingestion.open_meteo import (
    ARCHIVE_HOURLY_SCHEMA,
    ArchiveParseError,
    parse_archive_hourly,
)
from vn_climate_risk_monitor.ingestion.state import ClaimedObject


def _source(*, ward_keys: tuple[int, ...] = (11,)) -> ClaimedObject:
    timestamp = datetime(2026, 8, 21, 12, tzinfo=UTC)
    return ClaimedObject(
        file_id=UUID("00000000-0000-0000-0000-000000000101"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000102"),
        logical_run_id=UUID("00000000-0000-0000-0000-000000000103"),
        pipeline_name="open_meteo_archive",
        source_name="open_meteo",
        dataset="historical_weather_hourly",
        scope="canary_1",
        object_key="bronze/files/open_meteo/archive/year=2000/response.json",
        batch_index=0,
        size_bytes=100,
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
            "location_count": len(ward_keys),
            "request_granularity": "month",
        },
        file_parameters={
            "year": 2000,
            "month": 1,
            "start_date": "2000-01-01",
            "end_date": "2000-01-01",
            "location_batch_index": 0,
            "ward_keys": list(ward_keys),
        },
        expected_item_count=len(ward_keys),
        received_item_count=len(ward_keys),
    )


def _location(*, location_id: int | None = None) -> dict[str, object]:
    first = datetime(2000, 1, 1, tzinfo=UTC)
    times = [int((first + timedelta(hours=index)).timestamp()) for index in range(24)]
    result: dict[str, object] = {
        "latitude": 21.0,
        "longitude": 105.75,
        "elevation": 17.0,
        "generationtime_ms": 1.5,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "hourly_units": {
            "time": "unixtime",
            "precipitation": "mm",
            "rain": "mm",
            "weather_code": "wmo code",
            "soil_moisture_0_to_7cm": "m³/m³",
            "soil_moisture_7_to_28cm": "m³/m³",
        },
        "hourly": {
            "time": times,
            "precipitation": [float(index) / 10 for index in range(24)],
            "rain": [float(index) / 10 for index in range(24)],
            "weather_code": [0] * 24,
            "soil_moisture_0_to_7cm": [0.3] * 24,
            "soil_moisture_7_to_28cm": [0.35] * 24,
        },
    }
    if location_id is not None:
        result["location_id"] = location_id
    return result


def test_archive_parser_maps_strict_hourly_grain_and_schema() -> None:
    parsed = parse_archive_hourly(
        json.dumps(_location()).encode(),
        source=_source(),
        ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
    )

    assert parsed.table.schema == ARCHIVE_HOURLY_SCHEMA
    assert parsed.row_count == 24
    assert parsed.rescued_row_count == 0
    assert parsed.table.column("ward_key").to_pylist() == [11] * 24
    assert parsed.table.column("source_year").to_pylist() == [2000] * 24
    assert parsed.table.column("precipitation").to_pylist()[-1] == 2.3
    assert parsed.table.column("observed_time_utc").to_pylist()[0] == datetime(
        2000, 1, 1, tzinfo=UTC
    )


def test_archive_business_key_is_stable_across_collection_attempts() -> None:
    content = json.dumps(_location()).encode()
    first = parse_archive_hourly(
        content,
        source=_source(),
        ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
    )
    retried_source = replace(
        _source(),
        file_id=UUID("00000000-0000-0000-0000-000000000201"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000202"),
    )
    second = parse_archive_hourly(
        content,
        source=retried_source,
        ingested_at_utc=datetime(2026, 8, 22, 12, 5, tzinfo=UTC),
    )

    assert first.table.column("bronze_row_id").to_pylist() == second.table.column(
        "bronze_row_id"
    ).to_pylist()


def test_archive_parser_rejects_requested_variable_with_only_nulls() -> None:
    location = _location()
    hourly = location["hourly"]
    assert isinstance(hourly, dict)
    hourly["precipitation"] = [None] * 24

    with pytest.raises(ArchiveParseError, match="precipitation contains only null"):
        parse_archive_hourly(
            json.dumps(location).encode(),
            source=_source(),
            ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
        )


def test_archive_parser_rejects_non_contiguous_or_wrong_window() -> None:
    location = _location()
    hourly = location["hourly"]
    assert isinstance(hourly, dict)
    times = hourly["time"]
    assert isinstance(times, list)
    times[10] += 3600

    with pytest.raises(ArchiveParseError, match="contiguous hourly"):
        parse_archive_hourly(
            json.dumps(location).encode(),
            source=_source(),
            ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
        )


def test_archive_parser_maps_multi_location_order() -> None:
    payload = [_location(), _location(location_id=1)]
    source = _source(ward_keys=(11, 22))

    parsed = parse_archive_hourly(
        json.dumps(payload).encode(),
        source=source,
        ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
    )

    assert parsed.row_count == 48
    assert parsed.table.column("ward_key").to_pylist() == [11] * 24 + [22] * 24
    assert parsed.table.column("source_location_id").to_pylist()[-1] == 1


def test_archive_parser_rejects_date_metadata_outside_run_window() -> None:
    source = replace(
        _source(),
        file_parameters={
            "year": 2000,
            "month": 1,
            "start_date": date(2000, 1, 1).isoformat(),
            "end_date": date(2000, 1, 2).isoformat(),
            "location_batch_index": 0,
            "ward_keys": [11],
        },
    )

    with pytest.raises(ArchiveParseError, match="outside its run window"):
        parse_archive_hourly(
            json.dumps(_location()).encode(),
            source=source,
            ingested_at_utc=datetime(2026, 8, 21, 12, 5, tzinfo=UTC),
        )
