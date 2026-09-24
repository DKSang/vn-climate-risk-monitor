"""Start-time watermark pattern (ADR 0001), exercised against a real Postgres."""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from pipeline.job_run import create_meta_tables, job_run
from pipeline.settings import load_settings


@pytest.fixture(autouse=True)
def fresh_meta() -> None:
    with psycopg.connect(load_settings().postgres.dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS meta CASCADE")
    create_meta_tables()


def test_first_run_has_no_watermark_and_success_sets_it_to_its_start() -> None:
    with job_run("silver.stg_open_meteo_forecast") as first:
        assert first.watermark is None

    with job_run("silver.stg_open_meteo_forecast") as second:
        assert second.watermark == first.started_at


def test_failed_run_is_logged_and_keeps_the_previous_watermark() -> None:
    with job_run("silver.stg_open_meteo_forecast") as succeeded:
        pass

    with (
        pytest.raises(RuntimeError, match="cluster went down"),
        job_run("silver.stg_open_meteo_forecast"),
    ):
        raise RuntimeError("cluster went down")

    with job_run("silver.stg_open_meteo_forecast") as retry:
        assert retry.watermark == succeeded.started_at

    assert _job_runs() == [
        ("silver.stg_open_meteo_forecast", "success", None),
        ("silver.stg_open_meteo_forecast", "failed", "cluster went down"),
        ("silver.stg_open_meteo_forecast", "success", None),
    ]


def test_each_asset_has_its_own_watermark() -> None:
    with job_run("silver.stg_open_meteo_forecast"):
        pass

    with job_run("silver.clean_weather_forecast_hourly") as other:
        assert other.watermark is None


def test_resetting_the_watermark_by_hand_reprocesses_from_that_time() -> None:
    with job_run("silver.clean_weather_forecast_hourly"):
        pass
    reprocess_from = datetime(2026, 9, 1, tzinfo=UTC)
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        conn.execute(
            "UPDATE meta.watermarks SET watermark = %s WHERE asset = %s",
            (reprocess_from, "silver.clean_weather_forecast_hourly"),
        )

    with job_run("silver.clean_weather_forecast_hourly") as rerun:
        assert rerun.watermark == reprocess_from


def test_row_counts_set_by_the_caller_are_logged() -> None:
    with job_run("silver.stg_open_meteo_forecast") as run:
        run.rows_in = 9072
        run.rows_out = 9072

    with psycopg.connect(load_settings().postgres.dsn) as conn:
        counts = conn.execute("SELECT rows_in, rows_out FROM meta.job_runs").fetchall()
    assert counts == [(9072, 9072)]


def test_meta_tables_can_be_created_again_without_error() -> None:
    create_meta_tables()


def _job_runs() -> list[tuple]:
    with psycopg.connect(load_settings().postgres.dsn) as conn:
        return conn.execute(
            "SELECT asset, status, error FROM meta.job_runs ORDER BY id"
        ).fetchall()
