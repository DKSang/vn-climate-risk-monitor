"""Monthly Open-Meteo archive pipeline: one calendar month per run.

Each run processes its own data interval's month, so history is loaded with
Airflow's backfill, e.g.

    airflow dags backfill open_meteo_archive_monthly -s 2024-01-01 -e 2024-12-31

Fetching skips batches already in Bronze, so re-running a month is harmless.
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR

with DAG(
    dag_id="open_meteo_archive_monthly",
    default_args=DEFAULT_ARGS,
    description="Fetch, load, transform and publish one month of archive weather",
    # Day 6: Open-Meteo's archive trails real time by about five days.
    schedule="30 2 6 * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["archive", "monthly"],
) as dag:
    steps = {
        "init": "uv run init-lakehouse",
        "fetch": "uv run fetch-archive --month '{{ data_interval_start.strftime(\"%Y-%m-01\") }}'",
        "load": "uv run load-archive",
        "clean": "uv run build-clean-archive",
        "gold": "uv run build-gold-archive",
    }
    chain(
        *(
            BashOperator(task_id=name, pool=POOL, cwd=PROJECT_DIR, bash_command=command)
            for name, command in steps.items()
        )
    )
