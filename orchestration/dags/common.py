"""Shared Airflow configuration."""

from __future__ import annotations

import os
import subprocess
from typing import Any


def on_failure_alert(context: dict[str, Any]) -> None:
    """Run the health alert hook after a task failure."""
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


PROJECT_DIR = os.getenv("PROJECT_DIR", "/project")

# Serialize lakehouse writes and API-heavy tasks.
POOL = "lakehouse_single_writer_pool"

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": 180,
    "on_failure_callback": on_failure_alert,
}

PROVERO_CMD = "uv run provero run -c quality/provero.yaml --no-optimize --no-store"
PROVERO_ARCHIVE_CMD = (
    "uv run provero run -c quality/provero_archive.yaml --no-optimize --no-store"
)
