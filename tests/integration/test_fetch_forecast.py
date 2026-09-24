"""Landing the Open-Meteo forecast in Bronze, against a real S3 API (MinIO or moto).

HTTP is faked at the transport edge with `responses`; storage is real.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
import responses

from pipeline import lake
from pipeline.open_meteo.forecast import fetch_forecast
from pipeline.settings import load_settings

SLOT = datetime(2026, 9, 24, 10, 15, tzinfo=UTC)
RUN_PREFIX = (
    "bronze/open_meteo/forecast/year=2026/month=09/day=24/forecast_run=20260924T10/"
)
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


@pytest.fixture(autouse=True)
def empty_bucket() -> None:
    client = lake.minio_client()
    bucket = load_settings().minio.bucket
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    # Only Bronze: the rest of the bucket holds DuckLake data files other tests use.
    for obj in client.list_objects(bucket, prefix="bronze/", recursive=True):
        client.remove_object(bucket, obj.object_name)


def _bronze() -> dict[str, bytes]:
    client = lake.minio_client()
    bucket = load_settings().minio.bucket
    return {
        obj.object_name: client.get_object(bucket, obj.object_name).read()
        for obj in client.list_objects(bucket, prefix=RUN_PREFIX, recursive=True)
    }


def _body(n: int) -> bytes:
    return json.dumps([{"latitude": 21.0, "longitude": 105.8, "n": n}]).encode()


@responses.activate
def test_each_batch_of_grid_cells_lands_as_one_object_with_the_raw_bytes() -> None:
    responses.get(FORECAST_URL, body=_body(0))
    responses.get(FORECAST_URL, body=_body(1))

    keys = fetch_forecast(SLOT)

    # 48 ecmwf_ifs grid cells in batches of 25 -> 2 requests.
    assert keys == [RUN_PREFIX + "batch_000.json", RUN_PREFIX + "batch_001.json"]
    assert _bronze() == {keys[0]: _body(0), keys[1]: _body(1)}
    first_request = responses.calls[0].request.url
    assert "models=ecmwf_ifs" in first_request
    assert "forecast_hours=72" in first_request


@responses.activate
def test_rerunning_a_slot_neither_calls_the_api_nor_rewrites_bronze() -> None:
    responses.get(FORECAST_URL, body=_body(0))
    responses.get(FORECAST_URL, body=_body(1))
    fetch_forecast(SLOT)
    landed = _bronze()

    assert fetch_forecast(SLOT) == []
    assert len(responses.calls) == 2
    assert _bronze() == landed


@responses.activate
def test_an_open_meteo_error_payload_fails_and_lands_nothing() -> None:
    responses.get(
        FORECAST_URL,
        status=400,
        json={"error": True, "reason": "Cannot initialize WeatherVariable"},
    )

    with pytest.raises(RuntimeError, match="Cannot initialize WeatherVariable"):
        fetch_forecast(SLOT)
    assert _bronze() == {}


@responses.activate
def test_a_temporary_server_error_is_retried() -> None:
    responses.get(FORECAST_URL, status=503, body="Service Unavailable")
    responses.get(FORECAST_URL, body=_body(0))
    responses.get(FORECAST_URL, body=_body(1))

    keys = fetch_forecast(SLOT)

    assert _bronze() == {keys[0]: _body(0), keys[1]: _body(1)}
