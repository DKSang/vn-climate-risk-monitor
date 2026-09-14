"""Hourly forecast pipeline: copy, stage, dbt build, and health."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR

with DAG(
    dag_id="open_meteo_forecast_hourly",
    default_args=DEFAULT_ARGS,
    description="Fetch, stage, build and health-check the Open-Meteo forecast",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["forecast", "hourly"],
) as dag:
    copy_raw = BashOperator(
        task_id="copy_raw",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run fetch-open-meteo forecast --execute",
    )

    autoload_staging = BashOperator(
        task_id="autoload_staging",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run auto-loader forecast",
    )

    process_dbt = BashOperator(
        task_id="process_dbt",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run auto-process run forecast",
    )

    health = BashOperator(
        task_id="health",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run pipeline-health --scope forecast --require-gold",
    )

    copy_raw >> autoload_staging >> process_dbt >> health
