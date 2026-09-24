"""Land the hourly Open-Meteo forecast in Bronze, exactly as the API returned it."""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from io import BytesIO
from itertools import batched
from pathlib import Path

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from pipeline import lake
from pipeline.settings import load_settings

MODEL = "ecmwf_ifs"
HOURLY_FIELDS = "precipitation,rain,showers,precipitation_probability,weather_code"
RETRY_STATUSES = {429, 500, 502, 503, 504}
GRID_SEED = Path(__file__).parents[2] / "transform/seeds/ward_grid_map_seed.csv"


def fetch_forecast(slot: datetime) -> list[str]:
    """Fetch the forecast run for `slot` and return the Bronze keys it wrote."""
    slot = slot.astimezone(UTC)
    settings = load_settings()
    client = lake.minio_client()
    prefix = _run_prefix(slot)
    # Bronze is immutable: batches already landed (e.g. before an Airflow retry) are kept.
    landed = {
        obj.object_name
        for obj in client.list_objects(settings.minio.bucket, prefix=prefix)
    }
    keys = []
    for index, cells in enumerate(
        batched(_grid_cells(), settings.open_meteo.location_batch_size)
    ):
        key = f"{prefix}batch_{index:03}.json"
        if key in landed:
            continue
        body = _get(
            settings.open_meteo.forecast_url,
            {
                "latitude": ",".join(lat for lat, _ in cells),
                "longitude": ",".join(lon for _, lon in cells),
                "hourly": HOURLY_FIELDS,
                "models": MODEL,
                "forecast_hours": settings.open_meteo.forecast_hours,
                "timezone": "UTC",
                "timeformat": "unixtime",
            },
        )
        client.put_object(
            settings.minio.bucket,
            key,
            BytesIO(body),
            len(body),
            content_type="application/json",
        )
        keys.append(key)
    return keys


def _is_transient(error: BaseException) -> bool:
    if isinstance(error, requests.HTTPError):
        return error.response.status_code in RETRY_STATUSES
    return isinstance(error, requests.ConnectionError | requests.Timeout)


@retry(
    retry=retry_if_exception(_is_transient),
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _get(url: str, params: dict) -> bytes:
    response = requests.get(url, params=params, timeout=60)
    if response.status_code in RETRY_STATUSES:
        response.raise_for_status()
    payload = response.json()
    # Open-Meteo reports bad requests as {"error": true, "reason": ...}.
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(f"Open-Meteo: {payload.get('reason')}")
    response.raise_for_status()
    return response.content


def _run_prefix(slot: datetime) -> str:
    return (
        f"bronze/open_meteo/forecast/year={slot:%Y}/month={slot:%m}/day={slot:%d}/"
        f"forecast_run={slot:%Y%m%dT%H}/"
    )


def _grid_cells() -> list[tuple[str, str]]:
    with GRID_SEED.open(encoding="utf-8") as seed:
        cells = {
            (row["grid_latitude"], row["grid_longitude"])
            for row in csv.DictReader(seed)
            if row["model"] == MODEL
        }
    return sorted(cells)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--slot",
        type=datetime.fromisoformat,
        required=True,
        help="timezone-aware ISO time of the hourly slot, e.g. 2026-09-24T10:00:00+00:00",
    )
    slot = parser.parse_args().slot
    keys = fetch_forecast(slot)
    print(
        f"Landed {len(keys)} object(s) for forecast run {slot.astimezone(UTC):%Y%m%dT%H}"
    )
