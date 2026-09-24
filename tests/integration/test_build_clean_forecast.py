"""silver.stg_ -> silver.clean_weather_forecast_hourly with dbt, on the real stack."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from staging import stage

from pipeline import lake
from pipeline.init import main as init_lake
from pipeline.job_run import create_meta_tables
from pipeline.open_meteo.clean import build_clean_forecast
from pipeline.open_meteo.load import CREATE_STAGING
from pipeline.settings import load_settings

RUN = datetime(2026, 9, 24, 10, tzinfo=UTC)
ROWS_PER_RUN = 48 * 72


@pytest.fixture(autouse=True)
def empty_silver() -> None:
    init_lake()
    with lake.connect() as con:
        con.execute("DROP TABLE IF EXISTS silver.clean_weather_forecast_hourly")
        con.execute("DROP TABLE IF EXISTS silver.stg_open_meteo_forecast")
        con.execute(CREATE_STAGING)
    with psycopg.connect(load_settings().postgres.dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS meta CASCADE")
    create_meta_tables()


def clean() -> list[tuple]:
    with lake.connect() as con:
        return con.execute(
            """
            SELECT forecast_run, count(*), min(rain_mm), max(rain_mm)
            FROM silver.clean_weather_forecast_hourly
            GROUP BY forecast_run ORDER BY forecast_run
            """
        ).fetchall()


def test_a_key_staged_twice_is_kept_once_with_the_latest_values() -> None:
    first_load = datetime.now(UTC) - timedelta(minutes=10)
    stage(RUN, rain=0.4, inserted_at=first_load, file="batch_000.json")
    stage(
        RUN,
        rain=0.6,
        inserted_at=first_load + timedelta(minutes=1),
        file="batch_000.json",
    )

    assert build_clean_forecast() == ROWS_PER_RUN

    assert clean() == [(RUN, ROWS_PER_RUN, 0.6, 0.6)]


def timestamps(run: datetime) -> tuple:
    with lake.connect() as con:
        return con.execute(
            """
            SELECT min(_inserted_at), max(_inserted_at), min(_updated_at)
            FROM silver.clean_weather_forecast_hourly WHERE forecast_run = ?
            """,
            [run],
        ).fetchone()


def test_later_runs_read_only_newly_staged_rows_and_update_existing_keys() -> None:
    long_ago = datetime.now(UTC) - timedelta(days=1)
    stage(RUN, rain=0.4, inserted_at=long_ago, file="a.json")
    build_clean_forecast()
    first_inserted, _, first_updated = timestamps(RUN)

    next_run = RUN + timedelta(hours=1)
    stage(RUN, rain=0.9, inserted_at=datetime.now(UTC), file="b.json")
    stage(next_run, rain=1.0, inserted_at=datetime.now(UTC), file="c.json")
    # Staged before the watermark: already processed, must not be read again.
    stage(RUN + timedelta(hours=2), rain=5.0, inserted_at=long_ago, file="old.json")

    assert build_clean_forecast() == 2 * ROWS_PER_RUN

    assert clean() == [
        (RUN, ROWS_PER_RUN, 0.9, 0.9),
        (next_run, ROWS_PER_RUN, 1.0, 1.0),
    ]
    inserted_min, inserted_max, updated = timestamps(RUN)
    assert inserted_min == inserted_max == first_inserted
    assert updated > first_updated


def test_a_rerun_without_newly_staged_rows_changes_nothing() -> None:
    stage(
        RUN, rain=0.4, inserted_at=datetime.now(UTC) - timedelta(hours=1), file="a.json"
    )
    build_clean_forecast()
    before = timestamps(RUN)

    assert build_clean_forecast() == 0
    assert timestamps(RUN) == before


def test_rows_breaking_a_silver_expectation_fail_the_run_and_keep_the_watermark() -> (
    None
):
    stage(
        RUN, rain=0.4, inserted_at=datetime.now(UTC) - timedelta(hours=1), file="a.json"
    )
    build_clean_forecast()
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        watermark = conn.execute("SELECT watermark FROM meta.watermarks").fetchone()
    # WMO weather codes stop at 99; the Bronze gate does not check them.
    stage(
        RUN, rain=0.4, inserted_at=datetime.now(UTC), file="b.json", weather_code=150.0
    )

    with pytest.raises(RuntimeError, match="weather_code"):
        build_clean_forecast()

    with psycopg.connect(load_settings().postgres.dsn) as conn:
        assert (
            conn.execute("SELECT watermark FROM meta.watermarks").fetchone()
            == watermark
        )
        statuses = conn.execute(
            "SELECT status FROM meta.job_runs ORDER BY id"
        ).fetchall()
    assert statuses == [("success",), ("failed",)]
