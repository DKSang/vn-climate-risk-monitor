"""Hourly Open-Meteo forecast pipeline for Hanoi."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR, PROVERO_CMD

with DAG(
    dag_id="open_meteo_forecast_hourly",
    default_args=DEFAULT_ARGS,
    description="Fetch, stage and validate Open-Meteo hourly forecast for Hanoi",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["forecast", "hourly", "staging", "quality"],
) as dag:
    fetch_forecast = BashOperator(
        task_id="fetch_forecast",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run fetch-open-meteo forecast --execute",
    )

    load_staging = BashOperator(
        task_id="load_staging",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run load-sources open_meteo_forecast",
    )

    quality_provero = BashOperator(
        task_id="quality_provero",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=PROVERO_CMD,
    )

    quality_forecast = BashOperator(
        task_id="quality_forecast",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/healthcheck.py --scope forecast",
    )

    transform_forecast_silver = BashOperator(
        task_id="transform_forecast_silver",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run forecast_silver",
    )

    transform_forecast_gold = BashOperator(
        task_id="transform_forecast_gold",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run forecast_gold",
    )

    healthcheck = BashOperator(
        task_id="healthcheck_forecast",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "uv run python scripts/healthcheck.py --scope forecast --require-gold"
        ),
    )

    (
        fetch_forecast
        >> load_staging
        >> quality_provero
        >> quality_forecast
        >> transform_forecast_silver
        >> transform_forecast_gold
        >> healthcheck
    )
