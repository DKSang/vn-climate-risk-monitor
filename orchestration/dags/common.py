"""Hằng số và cấu hình dùng chung cho các DAGs của vn-climate-risk-monitor."""

from __future__ import annotations

import os
import subprocess
from typing import Any


def on_failure_alert(context: dict[str, Any]) -> None:
    """Callback gửi alert khi task Airflow thất bại.

    Gọi scripts/alert_health.py để gửi health report tới ALERT_WEBHOOK_URL
    nếu biến môi trường đó được thiết lập.
    """
    project_dir = os.getenv("PROJECT_DIR", "/project")
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"

    print(f"[ALERT] Task {dag_id}.{task_id} failed. Triggering alert_health.py...")
    subprocess.run(
        ["uv", "run", "python", "scripts/alert_health.py", "--scope", "all"],
        cwd=project_dir,
        check=False,
    )


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
    "on_failure_callback": on_failure_alert,
}

# Lệnh chạy Provero data quality gate (quét staging sau load)
# DUCKLAKE_DSN và cấu hình DuckLake đã được inject qua container environment
PROVERO_CMD = "uv run provero run -c quality/provero.yaml --no-optimize --no-store"
PROVERO_ARCHIVE_CMD = "uv run provero run -c quality/provero_archive.yaml --no-optimize --no-store"

