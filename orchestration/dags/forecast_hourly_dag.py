"""Hourly forecast pipeline: copy, stage, dbt build, and health."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR, on_processing_failure

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
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "SLOT='{{ data_interval_end.strftime(\"%Y-%m-%dT%H:00:00+00:00\") }}' && "
            "uv run fetch-open-meteo forecast --slot $SLOT --execute"
        ),
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "SLOT='{{ data_interval_end.strftime(\"%Y-%m-%dT%H:00:00+00:00\") }}' && "
            "uv run quality-gate raw forecast --slot $SLOT"
        ),
    )

    load_silver_staging = BashOperator(
        task_id="load_silver_staging",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run auto-loader forecast",
    )

    build_silver_intermediate = BashOperator(
        task_id="build_silver_intermediate",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run auto-process silver forecast --execution-key '{{ run_id }}'",
        do_xcom_push=True,
        params={"process_key": "forecast"},
        on_failure_callback=on_processing_failure,
    )

    validate_silver_intermediate = BashOperator(
        task_id="validate_silver_intermediate",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            'RUN_ID="{{ ti.xcom_pull(task_ids=\'build_silver_intermediate\') }}" && '
            'uv run auto-process vars forecast --run-id "$RUN_ID" >/dev/null && '
            "uv run quality-gate silver-int forecast"
        ),
        params={"process_key": "forecast"},
        on_failure_callback=on_processing_failure,
    )

    publish_gold = BashOperator(
        task_id="publish_gold",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            'RUN_ID="{{ ti.xcom_pull(task_ids=\'build_silver_intermediate\') }}" && '
            'STATE="$(uv run auto-process state forecast --run-id "$RUN_ID")" && '
            'if [ "$STATE" = "SUCCEEDED" ]; then exit 0; fi && '
            'DBT_VARS="$(uv run auto-process vars forecast --run-id "$RUN_ID")" && '
            'DBT_REFRESH="$(uv run auto-process refresh-flag forecast --run-id "$RUN_ID")" && '
            "uv run dbt build --project-dir transform --profiles-dir transform "
            "--indirect-selection cautious --select 'tag:marts,tag:forecast' "
            "$DBT_REFRESH --vars \"$DBT_VARS\" && "
            'uv run auto-process finalize forecast --run-id "$RUN_ID"'
        ),
        params={"process_key": "forecast"},
        on_failure_callback=on_processing_failure,
    )

    check_pipeline_health = BashOperator(
        task_id="check_pipeline_health",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run pipeline-health --scope forecast --require-gold",
    )

    ingest_bronze >> validate_bronze >> load_silver_staging >> build_silver_intermediate >> validate_silver_intermediate >> publish_gold >> check_pipeline_health
