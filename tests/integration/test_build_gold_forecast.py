"""silver.clean_ -> Gold forecast models, then publication, on the real stack."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from staging import stage

from pipeline import lake
from pipeline.init import main as init_lake
from pipeline.job_run import create_meta_tables
from pipeline.open_meteo.clean import build_clean_forecast
from pipeline.open_meteo.gold import build_gold_forecast
from pipeline.open_meteo.load import FORECAST_STAGING
from pipeline.settings import load_settings

NOW = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
ROWS_PER_RUN = 48 * 72
WARDS = 126


@pytest.fixture(autouse=True)
def empty_lake() -> None:
    init_lake()
    with lake.connect() as con:
        for table in (
            "gold.fct_rain_forecast_hourly",
            # A lake whose archive was never built must still build forecast Gold.
            "gold.fct_rain_archive_hourly",
            "gold._publications",
            "silver.clean_weather_forecast_hourly",
            "silver.stg_open_meteo_forecast",
        ):
            con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(FORECAST_STAGING)
    with psycopg.connect(load_settings().postgres.dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS meta CASCADE")
    create_meta_tables()


def stage_and_clean(run: datetime, rain: float = 0.4) -> None:
    stage(run, rain=rain, inserted_at=datetime.now(UTC), file=f"{run:%H}.json")
    build_clean_forecast()


def test_the_first_run_is_built_into_gold_and_published() -> None:
    started = datetime.now(UTC)
    stage_and_clean(NOW)

    build_gold_forecast()

    with lake.connect() as con:
        facts = con.execute(
            "SELECT forecast_run, count(*) FROM gold.fct_rain_forecast_hourly GROUP BY 1"
        ).fetchall()
        wards_per_hour = con.execute(
            """
            SELECT DISTINCT count(*) FROM gold.fct_rain_pressure_alert
            GROUP BY forecast_run, valid_at
            """
        ).fetchall()
        publications = con.execute("SELECT asset FROM gold._publications").fetchall()
        # Snapshot history outlives DROP TABLE, so count only this test's snapshots.
        publish_snapshots = con.execute(
            """
            SELECT count(*) FROM catalog1.snapshots()
            WHERE commit_message = 'publish' AND snapshot_time >= ?
            """,
            [started],
        ).fetchone()
    assert facts == [(NOW, ROWS_PER_RUN)]
    assert wards_per_hour == [(WARDS,)]
    assert publications == [("gold.fct_rain_forecast_hourly",)]
    assert publish_snapshots == (1,)


def history_updated_at() -> dict:
    with lake.connect() as con:
        return dict(
            con.execute(
                """
                SELECT forecast_run, max(_updated_at)
                FROM gold.fct_rain_forecast_hourly GROUP BY 1
                """
            ).fetchall()
        )


def test_a_new_run_recomputes_only_that_run() -> None:
    earlier = NOW - timedelta(hours=1)
    stage_and_clean(earlier)
    build_gold_forecast()
    before = history_updated_at()

    stage_and_clean(NOW)
    assert build_gold_forecast() == ROWS_PER_RUN

    after = history_updated_at()
    assert after[earlier] == before[earlier]
    assert set(after) == {earlier, NOW}


def test_a_failing_dbt_test_publishes_nothing_and_keeps_the_watermark() -> None:
    stage_and_clean(NOW)
    build_gold_forecast()
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        watermark = conn.execute(
            "SELECT watermark FROM meta.watermarks WHERE asset = 'gold.fct_rain_forecast_hourly'"
        ).fetchone()
    # A grid cell no ward maps to breaks the relationship test on the history fact.
    with lake.connect() as con:
        con.execute(
            """
            INSERT INTO silver.clean_weather_forecast_hourly
            SELECT * REPLACE ('not-a-grid-cell' AS grid_cell_id, now() AS _updated_at)
            FROM silver.clean_weather_forecast_hourly LIMIT 1
            """
        )

    with pytest.raises(RuntimeError, match="dbt build"):
        build_gold_forecast()

    with lake.connect() as con:
        assert con.execute("SELECT count(*) FROM gold._publications").fetchone() == (1,)
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        assert (
            conn.execute(
                "SELECT watermark FROM meta.watermarks WHERE asset = 'gold.fct_rain_forecast_hourly'"
            ).fetchone()
            == watermark
        )
