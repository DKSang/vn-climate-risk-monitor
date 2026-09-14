"""Build dbt command từ processing bounds và chạy dbt."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vn_climate_risk_monitor.auto_process.runner import Bounds

FILE_BACKED_DBT_SETTINGS = ("POSTGRES_PASSWORD", "MINIO_SECRET_KEY")


class DbtBuildError(RuntimeError):
    def __init__(self, command: Sequence[str], returncode: int) -> None:
        super().__init__(f"dbt exit {returncode}: {' '.join(command[:3])}")
        self.returncode = returncode


def to_sql_timestamp(value: datetime) -> str:
    """Format UTC timestamp dùng chung cho DuckDB/Postgres."""
    moment = value if value.tzinfo else value.replace(tzinfo=UTC)
    return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00")


def build_vars(bounds: Bounds) -> dict[str, Any]:
    """Vars cho incremental scope và run timestamp."""
    return {
        "processing_incremental": bounds.is_incremental,
        "processing_bounds": {
            source.source_ref: to_sql_timestamp(source.lower_bound)
            for source in bounds.sources
            if source.lower_bound is not None
        },
        "processing_run_started_at": to_sql_timestamp(bounds.run_started_at),
    }


def build_command(
    bounds: Bounds,
    *,
    selection: str | None = None,
    profiles_dir: str = ".",
) -> list[str]:
    command = [
        os.environ.get("DBT_EXECUTABLE", "dbt"),
        "build",
        "--profiles-dir",
        profiles_dir,
        "--indirect-selection",
        "cautious",
        "--vars",
        json.dumps(build_vars(bounds)),
    ]
    if not bounds.is_incremental:
        command.append("--full-refresh")
    if selection:
        command += ["--select", selection]
    return command


def _hydrate_file_backed_settings(environment: dict[str, str]) -> None:
    """Đọc `*_FILE` secrets vào env của dbt child process."""
    for name in FILE_BACKED_DBT_SETTINGS:
        secret_file = environment.get(f"{name}_FILE")
        if secret_file is None:
            continue
        try:
            value = Path(secret_file).read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as error:
            raise ValueError(f"Cannot read {name}_FILE: {secret_file}") from error
        if not value:
            raise ValueError(f"{name}_FILE must not be empty")
        environment[name] = value


def dbt_environment() -> dict[str, str]:
    """Build the child environment shared by all dbt entrypoints."""
    environment = os.environ.copy()
    _hydrate_file_backed_settings(environment)
    environment.setdefault(
        "DBT_LOG_PATH", str(Path(tempfile.gettempdir()) / "vn-climate-dbt-logs")
    )
    environment.setdefault(
        "DBT_TARGET_PATH", str(Path(tempfile.gettempdir()) / "vn-climate-dbt-target")
    )
    Path(environment["DBT_LOG_PATH"]).mkdir(parents=True, exist_ok=True)
    Path(environment["DBT_TARGET_PATH"]).mkdir(parents=True, exist_ok=True)
    return environment


def run_dbt(
    bounds: Bounds,
    *,
    project_dir: Path,
    selection: str | None = None,
    runner: Any = subprocess.run,
) -> None:
    """Chạy dbt; lỗi giữ nguyên processing checkpoint."""
    command = build_command(bounds, selection=selection)
    completed = runner(
        command,
        cwd=project_dir,
        check=False,
        env=dbt_environment(),
    )
    if completed.returncode != 0:
        raise DbtBuildError(command, completed.returncode)
