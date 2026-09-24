"""Bronze -> silver.stg_open_meteo_forecast, against real Postgres, S3 and DuckLake."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from io import BytesIO
from itertools import batched

import psycopg
import pytest

from pipeline import lake
from pipeline.init import main as init_lake
from pipeline.job_run import create_meta_tables
from pipeline.open_meteo.forecast import grid_cells, run_prefix
from pipeline.open_meteo.load import load_forecast
from pipeline.settings import load_settings

NOW = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
ROWS_PER_RUN = 48 * 72


@pytest.fixture(autouse=True)
def empty_lake() -> None:
    init_lake()
    with lake.connect() as con:
        con.execute("DROP TABLE IF EXISTS silver.stg_open_meteo_forecast")
    client, bucket = lake.minio_client(), load_settings().minio.bucket
    for obj in client.list_objects(bucket, prefix="bronze/", recursive=True):
        client.remove_object(bucket, obj.object_name)
    with psycopg.connect(load_settings().postgres.dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS meta CASCADE")
    create_meta_tables()


def land_run(slot: datetime, *, precipitation: float = 0.4) -> None:
    """Write one forecast run to Bronze the way Open-Meteo returns it."""
    hours = [int((slot + timedelta(hours=h)).timestamp()) for h in range(72)]
    client, bucket = lake.minio_client(), load_settings().minio.bucket
    for index, cells in enumerate(batched(grid_cells(), 25)):
        body = json.dumps(
            [
                {
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "hourly": {
                        "time": hours,
                        "precipitation": [precipitation] * 72,
                        "rain": [precipitation] * 72,
                        "showers": [0.0] * 72,
                        "precipitation_probability": [40] * 72,
                        "weather_code": [61] * 72,
                    },
                }
                for lat, lon in cells
            ]
        ).encode()
        key = f"{run_prefix(slot)}batch_{index:03}.json"
        client.put_object(bucket, key, BytesIO(body), len(body))
    # S3 LastModified has second precision; like a real fetch followed by a load,
    # make the next load start in a later second.
    time.sleep(1 - datetime.now(UTC).microsecond / 1_000_000)


def staged() -> list[tuple]:
    with lake.connect() as con:
        return con.execute(
            """
            SELECT forecast_run, count(*), count(DISTINCT _source_file),
                   count(_inserted_at)
            FROM silver.stg_open_meteo_forecast
            GROUP BY forecast_run ORDER BY forecast_run
            """
        ).fetchall()


def test_new_bronze_files_are_appended_to_staging_with_lineage() -> None:
    land_run(NOW)

    assert load_forecast() == ROWS_PER_RUN

    assert staged() == [(NOW, ROWS_PER_RUN, 2, ROWS_PER_RUN)]
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        logged = conn.execute("SELECT status, rows_out FROM meta.job_runs").fetchall()
    assert logged == [("success", ROWS_PER_RUN)]


def test_a_rerun_without_new_files_appends_nothing() -> None:
    land_run(NOW)
    load_forecast()

    assert load_forecast() == 0
    assert staged() == [(NOW, ROWS_PER_RUN, 2, ROWS_PER_RUN)]


def test_a_file_breaking_a_bronze_expectation_fails_the_load_and_appends_nothing() -> (
    None
):
    land_run(NOW - timedelta(hours=1))
    load_forecast()
    land_run(NOW, precipitation=-5.0)

    with pytest.raises(RuntimeError, match="precipitation"):
        load_forecast()

    assert staged() == [(NOW - timedelta(hours=1), ROWS_PER_RUN, 2, ROWS_PER_RUN)]
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        statuses = conn.execute(
            "SELECT status FROM meta.job_runs ORDER BY id"
        ).fetchall()
    assert statuses == [("success",), ("failed",)]


def test_runs_waiting_after_an_outage_are_loaded_together() -> None:
    land_run(NOW - timedelta(hours=1))
    land_run(NOW)

    assert load_forecast() == 2 * ROWS_PER_RUN
    assert [run for run, *_ in staged()] == [NOW - timedelta(hours=1), NOW]


def test_a_stale_newest_forecast_fails_the_freshness_check() -> None:
    land_run(NOW - timedelta(days=2))

    with pytest.raises(RuntimeError, match="newest run"):
        load_forecast()
    with lake.connect() as con:
        assert con.execute(
            "SELECT count(*) FROM silver.stg_open_meteo_forecast"
        ).fetchone() == (0,)
