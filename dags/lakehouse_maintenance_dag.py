"""Daily DuckLake housekeeping, kept apart from the data DAGs."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR

with DAG(
    dag_id="lakehouse_maintenance_daily",
    default_args=DEFAULT_ARGS,
    description="Expire DuckLake snapshots and clean old Parquet files safely",
    schedule="30 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "maintenance", "retention"],
) as dag:
    maintain_lakehouse = BashOperator(
        task_id="maintain_lakehouse",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run maintain-lakehouse",
    )
