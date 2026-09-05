"""DAG chạy pipeline dự báo thời tiết Open-Meteo hàng giờ cho 126 phường Hà Nội.

Đặc tính:
- Cadence: Hàng giờ vào phút thứ 15 (`15 * * * *`).
- Flow tuần tự: fetch -> load staging -> provero quality -> healthcheck.
- Cổng chặn: Nếu quality gate hoặc healthcheck fail, pipeline dừng lại ngay.
- Khóa tài nguyên: Sử dụng pool `lakehouse_single_writer_pool` (1 slot) để
  tránh xung đột ghi DuckLake hoặc dẫm quota với job archive.
"""

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

    # 1. Fetch JSON từ Open-Meteo và land lên MinIO bronze/files/...
    fetch_forecast = BashOperator(
        task_id="fetch_forecast",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run fetch-open-meteo forecast --execute",
    )

    # 2. Autoloader quét MinIO và nạp file mới vào silver.stg_weather_forecast
    load_staging = BashOperator(
        task_id="load_staging",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run load-sources open_meteo_forecast",
    )

    # 3. Provero quality gate: quét bảng staging ngay sau load (exit 1 khi fail)
    quality_provero = BashOperator(
        task_id="quality_provero",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=PROVERO_CMD,
    )

    # 4. Transform: chạy forecast_gold process (staging view -> intermediate -> fct_rain_forecast_hourly)
    transform_forecast = BashOperator(
        task_id="transform_forecast",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run forecast_gold",
    )

    # 5. Kiểm tra sức khỏe pipeline forecast
    healthcheck = BashOperator(
        task_id="healthcheck_forecast",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/healthcheck.py --scope forecast",
    )

    fetch_forecast >> load_staging >> quality_provero >> transform_forecast >> healthcheck
