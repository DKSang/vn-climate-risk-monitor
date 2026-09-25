"""Archive pipeline: one Open-Meteo month from Bronze through Gold, on the real stack.

Behaviour shared with the forecast (skip already-landed batches, retries,
watermarks) is tested once, in the forecast tests.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from itertools import batched

import psycopg
import pytest
import responses
import streamlit as st

from dashboard import queries
from pipeline import lake
from pipeline.init import main as init_lake
from pipeline.job_run import create_meta_tables
from pipeline.open_meteo.clean import build_clean_archive
from pipeline.open_meteo.fetch import fetch_archive, grid_cells
from pipeline.open_meteo.gold import build_gold_archive
from pipeline.open_meteo.load import load_archive
from pipeline.settings import load_settings

MONTH = date(2024, 6, 1)
PREFIX = "bronze/open_meteo/archive/year=2024/month=06/"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURS_IN_JUNE = 30 * 24


@pytest.fixture(autouse=True)
def empty_archive() -> None:
    init_lake()
    with lake.connect() as con:
        for table in (
            "gold.fct_rain_archive_hourly",
            # A lake whose forecast was never built must still build archive Gold.
            "gold.fct_rain_forecast_hourly",
            "silver.clean_weather_archive_hourly",
            "silver.stg_open_meteo_archive",
        ):
            con.execute(f"DROP TABLE IF EXISTS {table}")
    client, bucket = lake.minio_client(), load_settings().minio.bucket
    for obj in client.list_objects(bucket, prefix="bronze/", recursive=True):
        client.remove_object(bucket, obj.object_name)
    with psycopg.connect(load_settings().postgres.dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS meta CASCADE")
    create_meta_tables()


def month_body(cells: tuple, month: date, *, rain: float = 0.2) -> bytes:
    """An Open-Meteo archive response for `cells` over the whole of `month`."""
    start = datetime(month.year, month.month, 1, tzinfo=UTC)
    end = datetime(month.year + month.month // 12, month.month % 12 + 1, 1, tzinfo=UTC)
    hours = int((end - start).total_seconds() // 3600)
    times = [int((start + timedelta(hours=h)).timestamp()) for h in range(hours)]
    return json.dumps(
        [
            {
                "latitude": float(lat),
                "longitude": float(lon),
                "hourly": {
                    "time": times,
                    "precipitation": [rain] * hours,
                    "rain": [rain] * hours,
                    "weather_code": [61] * hours,
                },
            }
            for lat, lon in cells
        ]
    ).encode()


def land_month(month: date, *, rain: float = 0.2, batches: int | None = None) -> None:
    client, bucket = lake.minio_client(), load_settings().minio.bucket
    prefix = f"bronze/open_meteo/archive/year={month:%Y}/month={month:%m}/"
    for index, cells in enumerate(list(batched(grid_cells(), 25))[:batches]):
        body = month_body(cells, month, rain=rain)
        client.put_object(
            bucket, f"{prefix}batch_{index:03}.json", BytesIO(body), len(body)
        )


@responses.activate
def test_a_month_of_archive_lands_under_its_year_and_month() -> None:
    cells = list(batched(grid_cells(), 25))
    responses.get(ARCHIVE_URL, body=month_body(cells[0], MONTH))
    responses.get(ARCHIVE_URL, body=month_body(cells[1], MONTH))

    keys = fetch_archive(MONTH)

    assert keys == [PREFIX + "batch_000.json", PREFIX + "batch_001.json"]
    request = responses.calls[0].request.url
    assert "start_date=2024-06-01" in request
    assert "end_date=2024-06-30" in request
    assert "models=ecmwf_ifs" in request


def staged_archive() -> list[tuple]:
    with lake.connect() as con:
        return con.execute(
            """
            SELECT count(*), count(DISTINCT _source_file), min(valid_at), max(valid_at)
            FROM silver.stg_open_meteo_archive
            """
        ).fetchone()


def test_a_landed_month_is_appended_to_staging_with_lineage() -> None:
    land_month(MONTH)

    assert load_archive() == 48 * HOURS_IN_JUNE

    rows, files, first, last = staged_archive()
    assert (rows, files) == (48 * HOURS_IN_JUNE, 2)
    assert first == datetime(2024, 6, 1, tzinfo=UTC)
    assert last == datetime(2024, 6, 30, 23, tzinfo=UTC)


def test_a_month_missing_grid_cells_fails_the_volume_check() -> None:
    land_month(MONTH, batches=1)  # only the first 25 of 48 grid cells

    with pytest.raises(RuntimeError, match="row_count"):
        load_archive()
    with lake.connect() as con:
        assert con.execute(
            "SELECT count(*) FROM silver.stg_open_meteo_archive"
        ).fetchone() == (0,)


def test_rain_windows_run_across_month_boundaries_and_gold_is_published() -> None:
    started = datetime.now(UTC)
    land_month(date(2024, 5, 1), rain=0.5)
    land_month(MONTH, rain=0.5)
    load_archive()
    build_clean_archive()

    build_gold_archive()

    with lake.connect() as con:
        rows, cells = con.execute(
            "SELECT count(*), count(DISTINCT grid_cell_id) FROM gold.fct_rain_archive_hourly"
        ).fetchone()
        first_of_june = con.execute(
            """
            SELECT DISTINCT rain_24h_mm FROM gold.fct_rain_archive_hourly
            WHERE valid_at = '2024-06-01 00:00:00+00'
            """
        ).fetchall()
        published = con.execute(
            "SELECT asset FROM gold._publications WHERE published_at >= ?", [started]
        ).fetchall()
    assert (rows, cells) == (48 * (31 + 30) * 24, 48)
    # 23 hours of May plus the first hour of June: the window crosses the months.
    assert first_of_june == [(12.0,)]
    assert published == [("gold.fct_rain_archive_hourly",)]


def test_the_dashboard_reads_a_published_month_by_ward_and_hour() -> None:
    land_month(MONTH)
    load_archive()
    build_clean_archive()
    build_gold_archive()
    st.cache_data.clear()
    snapshot = queries.published_snapshot()

    assert date(2024, 6, 1) in queries.archive_months(snapshot)
    month = queries.archive_month(snapshot, date(2024, 6, 1))
    assert len(month) == 126 * HOURS_IN_JUNE
    assert month["ward_code"].nunique() == 126
    assert (month["precipitation_mm"] == 0.2).all()
