"""Hourly Open-Meteo forecast pipeline.

Rebuild in progress: load, transform and publish tasks are added by later phases.
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR

with DAG(
    dag_id="open_meteo_forecast_hourly",
    default_args=DEFAULT_ARGS,
    description="Fetch, load, transform and publish the Open-Meteo forecast",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["forecast", "hourly"],
) as dag:
    init = BashOperator(
        task_id="init",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run init-lakehouse",
    )

    fetch = BashOperator(
        task_id="fetch",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run fetch-forecast --slot '{{ data_interval_end.isoformat() }}'",
    )

    init >> fetch
