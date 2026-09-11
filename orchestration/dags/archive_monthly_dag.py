"""Monthly ERA5/IFS archive backfill and Gold refresh."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR, PROVERO_ARCHIVE_CMD

with DAG(
    dag_id="open_meteo_archive_monthly",
    default_args=DEFAULT_ARGS,
    description="Monthly incremental backfill for ERA5/IFS and refresh Gold marts",
    schedule="30 2 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["archive", "monthly", "gold", "transform"],
) as dag:
    fetch_archive = BashOperator(
        task_id="fetch_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "START=$(date -d '{{ data_interval_start.strftime(\"%Y-%m-01\") }} - 1 month' +%Y-%m-01) && "
            'END={{ data_interval_start.strftime("%Y-%m-01") }} && '
            'echo "Fetching archive window: $START to $END" && '
            "uv run fetch-open-meteo archive --start $START --end $END --execute"
        ),
    )

    load_archive = BashOperator(
        task_id="load_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run load-sources open_meteo_archive open_meteo_ifs",
    )

    quality_provero_archive = BashOperator(
        task_id="quality_provero_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=PROVERO_ARCHIVE_CMD,
    )

    quality_archive = BashOperator(
        task_id="quality_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/healthcheck.py --scope archive",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "uv run python scripts/run_dbt.py seed "
            "--project-dir transform --profiles-dir transform"
        ),
    )

    transform_silver = BashOperator(
        task_id="transform_silver_weather",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run silver_weather",
    )

    transform_gold = BashOperator(
        task_id="transform_rain_gold",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run rain_gold",
    )

    healthcheck_gold = BashOperator(
        task_id="healthcheck_gold",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/healthcheck.py --scope archive --require-gold",
    )

    (
        fetch_archive
        >> load_archive
        >> quality_provero_archive
        >> quality_archive
        >> dbt_seed
        >> transform_silver
        >> transform_gold
        >> healthcheck_gold
    )
