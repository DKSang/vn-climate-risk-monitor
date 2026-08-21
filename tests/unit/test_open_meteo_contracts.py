import json
from datetime import UTC, datetime, timedelta

import pytest

from vn_climate_risk_monitor.ingestion.open_meteo import (
    FORECAST_HOURLY_VARIABLES,
    ForecastRequestContract,
    RequestedLocation,
    split_location_batches,
)


def _location(ward_key: int, ward_code: str) -> RequestedLocation:
    return RequestedLocation(
        ward_key=ward_key,
        ward_code=ward_code,
        latitude=21.0 + ward_key / 1000,
        longitude=105.8 + ward_key / 1000,
    )


def test_location_batches_are_sorted_unique_and_bounded() -> None:
    locations = (_location(3, "00003"), _location(1, "00001"), _location(2, "00002"))

    batches = split_location_batches(locations, batch_size=2)

    assert [[item.ward_key for item in batch] for batch in batches] == [[1, 2], [3]]


def test_location_batches_reject_duplicate_business_keys() -> None:
    with pytest.raises(ValueError, match="ward_key"):
        split_location_batches((_location(1, "00001"), _location(1, "00002")), 25)


def test_forecast_request_preserves_order_and_builds_api_parameters() -> None:
    scheduled_at = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    contract = ForecastRequestContract(
        endpoint="https://api.open-meteo.com/v1/forecast",
        model="best_match",
        forecast_hours=72,
        batch_index=0,
        scheduled_at_utc=scheduled_at,
        requested_at_utc=scheduled_at + timedelta(seconds=3),
        locations=(_location(1, "00001"), _location(2, "00002")),
    )

    payload = json.loads(contract.to_json_bytes())

    assert contract.api_query_params == {
        "latitude": "21.001,21.002",
        "longitude": "105.801,105.802",
        "hourly": ",".join(FORECAST_HOURLY_VARIABLES),
        "models": "best_match",
        "forecast_hours": 72,
        "timeformat": "unixtime",
        "timezone": "GMT",
        "precipitation_unit": "mm",
        "cell_selection": "land",
    }
    assert payload["scheduled_at_utc"] == "2026-08-21T08:15:00Z"
    assert payload["requested_at_utc"] == "2026-08-21T08:15:03Z"
    assert [item["request_location_index"] for item in payload["locations"]] == [0, 1]
    assert [item["ward_key"] for item in payload["locations"]] == [1, 2]


def test_forecast_request_rejects_naive_timestamps() -> None:
    with pytest.raises(ValueError, match="scheduled_at_utc"):
        ForecastRequestContract(
            endpoint="https://api.open-meteo.com/v1/forecast",
            model="best_match",
            forecast_hours=72,
            batch_index=0,
            scheduled_at_utc=datetime(2026, 8, 21, 8, 15),  # noqa: DTZ001
            requested_at_utc=datetime(2026, 8, 21, 8, 16, tzinfo=UTC),
            locations=(_location(1, "00001"),),
        )


def test_forecast_request_rejects_unsorted_locations() -> None:
    scheduled_at = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    with pytest.raises(ValueError, match="ordered by ward_key"):
        ForecastRequestContract(
            endpoint="https://api.open-meteo.com/v1/forecast",
            model="best_match",
            forecast_hours=72,
            batch_index=0,
            scheduled_at_utc=scheduled_at,
            requested_at_utc=scheduled_at,
            locations=(_location(2, "00002"), _location(1, "00001")),
        )


def test_forecast_request_rejects_request_before_schedule() -> None:
    scheduled_at = datetime(2026, 8, 21, 8, 15, 0, 900_000, tzinfo=UTC)
    with pytest.raises(ValueError, match="must not precede"):
        ForecastRequestContract(
            endpoint="https://api.open-meteo.com/v1/forecast",
            model="best_match",
            forecast_hours=72,
            batch_index=0,
            scheduled_at_utc=scheduled_at,
            requested_at_utc=scheduled_at - timedelta(microseconds=1),
            locations=(_location(1, "00001"),),
        )
