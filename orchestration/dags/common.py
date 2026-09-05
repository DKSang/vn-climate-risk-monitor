"""Hằng số và cấu hình dùng chung cho các DAGs của vn-climate-risk-monitor."""

from __future__ import annotations

import os

# Thư mục gốc repo bên trong container (volume mount: .:/project)
PROJECT_DIR = os.getenv("PROJECT_DIR", "/project")

# Airflow Pool 1 slot thay thế flock — tránh xung đột DuckLake và quota API
POOL = "lakehouse_single_writer_pool"

# Default arguments chuẩn cho mọi DAG
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": 180,  # 3 phút
}

# Lệnh chạy Provero data quality gate (quét staging sau load)
# DUCKLAKE_DSN và cấu hình DuckLake đã được inject qua container environment
PROVERO_CMD = "uv run provero run -c quality/provero.yaml --no-optimize --no-store"
