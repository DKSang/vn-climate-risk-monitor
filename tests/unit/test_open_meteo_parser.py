import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import pytest

from vn_climate_risk_monitor.ingestion.open_meteo import (
    FORECAST_HOURLY_SCHEMA,
    ForecastParseError,
    parse_forecast_hourly,
)
from vn_climate_risk_monitor.ingestion.state import ClaimedObject


def _source(*, ward_keys: tuple[int, ...] = (11, 22)) -> ClaimedObject:
    timestamp = datetime(2026, 8, 21, 10, 15, tzinfo=UTC)
    return ClaimedObject(
        file_id=UUID("00000000-0000-0000-0000-000000000001"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000002"),
        logical_run_id=UUID("00000000-0000-0000-0000-000000000003"),
        pipeline_name="open_meteo_forecast",
        source_name="open_meteo",
        dataset="forecast",
        scope="test",
        object_key="bronze/files/open_meteo/forecast/response_000.json",
        batch_index=0,
        size_bytes=100,
        sha256="a" * 64,
        content_type="application/json",
        retry_count=0,
        scheduled_at_utc=timestamp,
        collection_started_at_utc=timestamp,
        collection_completed_at_utc=timestamp,
        source_uri="https://api.open-meteo.test/v1/forecast",
        collector_version="0.2.0",
        contract_version="1",
        run_parameters={
            "model": "best_match",
            "forecast_hours": 72,
            "hourly_variables": [
                "precipitation",
                "rain",
                "showers",
                "precipitation_probability",
                "weather_code",
            ],
            "location_count": len(ward_keys),
        },
        file_parameters={"ward_keys": list(ward_keys)},
        expected_item_count=len(ward_keys),
        received_item_count=len(ward_keys),
    )


def _location(
    *,
    latitude: float,
    first_time: int = 1_787_306_400,
    location_id: int | None = None,
) -> dict[str, object]:
    result = {
        "latitude": latitude,
        "longitude": 105.8,
        "elevation": 10.0,
        "generationtime_ms": 0.1,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "hourly_units": {
            "time": "unixtime",
            "precipitation": "mm",
            "rain": "mm",
            "showers": "mm",
            "precipitation_probability": "%",
            "weather_code": "wmo code",
        },
        "hourly": {
            "time": [first_time, first_time + 3600],
            "precipitation": [1.2, 2.3],
            "rain": [1.0, 2.0],
            "showers": [0.2, 0.3],
            "precipitation_probability": [80, 90],
            "weather_code": [61, 63],
        },
    }
    if location_id is not None:
        result["location_id"] = location_id
    return result


def test_parser_maps_ordered_locations_and_hourly_grain() -> None:
    payload = [_location(latitude=21.1), _location(latitude=21.2, location_id=1)]

    parsed = parse_forecast_hourly(
        json.dumps(payload).encode(),
        source=_source(),
        ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
    )

    assert parsed.table.schema == FORECAST_HOURLY_SCHEMA
    assert parsed.row_count == 4
    assert parsed.rescued_row_count == 0
    assert parsed.table.column("ward_key").to_pylist() == [11, 11, 22, 22]
    assert parsed.table.column("request_location_index").to_pylist() == [0, 0, 1, 1]
    assert parsed.table.column("source_location_id").to_pylist() == [
        None,
        None,
        1,
        1,
    ]
    assert parsed.table.column("precipitation").to_pylist() == [1.2, 2.3, 1.2, 2.3]
    assert len(set(parsed.table.column("bronze_row_id").to_pylist())) == 4


def test_parser_preserves_array_mismatch_and_unknown_values_in_rescued_data() -> None:
    location = _location(latitude=21.1)
    hourly = location["hourly"]
    assert isinstance(hourly, dict)
    hourly["precipitation"] = [1.2]
    hourly["soil_moisture"] = [0.1, 0.2, 0.3]

    parsed = parse_forecast_hourly(
        json.dumps(location).encode(),
        source=_source(ward_keys=(11,)),
        ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
    )

    assert parsed.row_count == 3
    assert parsed.rescued_row_count == 3
    assert parsed.table.column("precipitation").to_pylist() == [1.2, None, None]
    assert parsed.table.column("valid_time_utc").to_pylist()[2] is None
    rescued = json.loads(parsed.table.column("_rescued_data")[2].as_py())
    assert rescued["unknown_hourly_values"]["soil_moisture"] == 0.3
    assert "time" in rescued["missing_hourly_values"]


def test_parser_rescues_invalid_known_value_as_null() -> None:
    location = _location(latitude=21.1)
    hourly = location["hourly"]
    assert isinstance(hourly, dict)
    hourly["weather_code"] = ["storm", 63]

    parsed = parse_forecast_hourly(
        json.dumps(location).encode(),
        source=_source(ward_keys=(11,)),
        ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
    )

    assert parsed.table.column("weather_code").to_pylist() == [None, 63]
    rescued = json.loads(parsed.table.column("_rescued_data")[0].as_py())
    assert rescued["invalid_hourly_values"]["weather_code"] == "storm"


def test_parser_rejects_non_monotonic_time() -> None:
    location = _location(latitude=21.1)
    hourly = location["hourly"]
    assert isinstance(hourly, dict)
    hourly["time"] = [1_787_310_000, 1_787_306_400]

    with pytest.raises(ForecastParseError, match="strictly increasing"):
        parse_forecast_hourly(
            json.dumps(location).encode(),
            source=_source(ward_keys=(11,)),
            ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
        )


def test_parser_rejects_response_location_count_mismatch() -> None:
    with pytest.raises(ForecastParseError, match="Expected 2 locations"):
        parse_forecast_hourly(
            json.dumps(_location(latitude=21.1)).encode(),
            source=_source(),
            ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
        )


def test_parser_rejects_location_id_that_breaks_ordered_mapping() -> None:
    payload = [_location(latitude=21.1), _location(latitude=21.2, location_id=9)]

    with pytest.raises(ForecastParseError, match="does not match response order"):
        parse_forecast_hourly(
            json.dumps(payload).encode(),
            source=_source(),
            ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
        )


def test_parser_rejects_generic_context_without_forecast_contract() -> None:
    source = replace(_source(), file_parameters={"station_ids": ["HN001"]})

    with pytest.raises(ForecastParseError, match="Invalid forecast control metadata"):
        parse_forecast_hourly(
            json.dumps(_location(latitude=21.1)).encode(),
            source=source,
            ingested_at_utc=datetime(2026, 8, 21, 10, 20, tzinfo=UTC),
        )
