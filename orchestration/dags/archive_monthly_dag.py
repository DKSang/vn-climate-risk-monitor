"""Monthly archive pipeline: copy, stage, dbt build, and health."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR

with DAG(
    dag_id="open_meteo_archive_monthly",
    default_args=DEFAULT_ARGS,
    description="Monthly archive backfill and dbt Gold refresh",
    schedule="30 2 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["archive", "monthly"],
) as dag:
    copy_raw = BashOperator(
        task_id="copy_raw",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "MONTH=$(date -d '{{ data_interval_start.strftime(\"%Y-%m-01\") }} - 1 month' +%Y-%m-01) && "
            'echo "Fetching archive month: $MONTH" && '
            "uv run fetch-open-meteo archive --start $MONTH --end $MONTH --execute"
        ),
    )

    autoload_staging = BashOperator(
        task_id="autoload_staging",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run auto-loader archive",
    )

    quality_ingest = BashOperator(
        task_id="quality_ingest",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run quality-gate ingest archive",
    )

    process_dbt = BashOperator(
        task_id="process_dbt",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run auto-process run archive",
    )

    health = BashOperator(
        task_id="health",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run pipeline-health --scope archive --require-gold",
    )

    copy_raw >> autoload_staging >> quality_ingest >> process_dbt >> health
