"""DAG định kỳ bồi đuôi dữ liệu lịch sử (ERA5 + ECMWF IFS) và làm mới Gold Marts.

Đặc tính:
- Cadence: Hàng tháng vào lúc 02:30 ngày mùng 1 hàng tháng (`30 2 1 * *`).
- Cửa sổ đọc: Tự động lùi 2 tháng đến hiện tại để vét late-arriving data.
- Idempotency: Planner tự động bỏ qua tháng đã đủ giờ trong Bronze.
- Flow trọn vẹn: Fetch -> Autoloader -> Quality -> Seed -> Transform Silver
  (change-aware merge) -> Transform Gold -> Quality Gold.
- Khóa tài nguyên: Sử dụng pool `lakehouse_single_writer_pool` (1 slot).
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import DEFAULT_ARGS, POOL, PROJECT_DIR, PROVERO_ARCHIVE_CMD

archive_args = {
    **DEFAULT_ARGS,
    "depends_on_past": True,  # Đảm bảo tuần tự các tháng khi backfill
}

with DAG(
    dag_id="open_meteo_archive_monthly",
    default_args=archive_args,
    description="Monthly incremental backfill for ERA5/IFS and refresh Gold marts",
    schedule="30 2 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["archive", "monthly", "gold", "transform"],
) as dag:

    # 1. Fetch dữ liệu lịch sử theo ô lưới (lùi 2 tháng để vét late data)
    # Jinja template: ds là ngày execution_date dạng YYYY-MM-DD
    fetch_archive = BashOperator(
        task_id="fetch_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=(
            "START=$(date -d '{{ data_interval_start.strftime(\"%Y-%m-01\") }} - 1 month' +%Y-%m-01) && "
            "END={{ data_interval_end.strftime(\"%Y-%m-01\") }} && "
            "echo \"Fetching archive window: $START to $END\" && "
            "uv run fetch-open-meteo archive --start $START --end $END --execute"
        ),
    )

    # 2. Autoloader nạp cả 2 nguồn era5 và ecmwf_ifs vào staging
    load_archive = BashOperator(
        task_id="load_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command='uv run load-sources "open_meteo_archive open_meteo_ifs"',
    )

    # 3a. Provero quality gate: quét bảng staging archive ngay sau load
    quality_provero_archive = BashOperator(
        task_id="quality_provero_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command=PROVERO_ARCHIVE_CMD,
    )

    # 3b. Quality check tổng quan cho layer staging archive
    quality_archive = BashOperator(
        task_id="quality_archive",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/healthcheck.py --scope archive",
    )

    # 4. Cập nhật seed địa lý nếu có thay đổi
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        pool=POOL,
        cwd=f"{PROJECT_DIR}/transform",
        bash_command="uv run dbt seed --profiles-dir .",
    )

    # 5. Transform Silver (curated) - Dedup + change-aware MERGE
    transform_silver = BashOperator(
        task_id="transform_silver_weather",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run silver_weather",
    )

    # 6. Transform Gold - Rolling windows + KPI bands
    transform_gold = BashOperator(
        task_id="transform_rain_gold",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/run_processing.py run rain_gold",
    )

    # 7. Healthcheck toàn diện kiểm định Gold layer
    healthcheck_gold = BashOperator(
        task_id="healthcheck_gold",
        pool=POOL,
        cwd=PROJECT_DIR,
        bash_command="uv run python scripts/healthcheck.py --scope archive --require-gold",
    )

    # Maintenance chạy ở DAG lakehouse_maintenance_daily, không gắn với build.
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
