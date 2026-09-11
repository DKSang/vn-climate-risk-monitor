"""Runner dbt: biến ``Bounds`` thành ``dbt build --vars``.

Framework KHÔNG thay dbt. dbt vẫn giữ DAG, ``ref()``, MERGE và tests; framework
chỉ sở hữu checkpoint, audit, và việc bơm cửa sổ đọc xuống cho model.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from processing.runner import Bounds

FILE_BACKED_DBT_SETTINGS = ("POSTGRES_PASSWORD", "MINIO_SECRET_KEY")


class DbtBuildError(RuntimeError):
    def __init__(self, command: Sequence[str], returncode: int) -> None:
        super().__init__(f"dbt exit {returncode}: {' '.join(command[:3])}")
        self.returncode = returncode


def to_sql_timestamp(value: datetime) -> str:
    """Format DuckDB/Postgres luôn parse được, kể cả khi input naive.

    Tránh ``isoformat()``: nó sinh ``T`` và ``+00:00``, còn text format chuẩn của
    cả hai engine là dấu cách và offset hai chữ số.
    """
    moment = value if value.tzinfo else value.replace(tzinfo=UTC)
    return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00")


def build_vars(bounds: Bounds) -> dict[str, Any]:
    """Var mà macro ``incremental_scope`` đọc.

    ``processing_incremental`` tách khỏi ``processing_bounds`` vì materialization
    cần biết full-refresh hay không TRƯỚC khi biết model đọc source nào.
    """
    return {
        "processing_incremental": bounds.is_incremental,
        "processing_bounds": {
            source.source_ref: to_sql_timestamp(source.lower_bound)
            for source in bounds.sources
            if source.lower_bound is not None
        },
        # Dấu thời gian cho `_updated_at` của các lớp mutable. Lấy start time của
        # run, KHÔNG phải CURRENT_TIMESTAMP của DuckDB: cùng kỷ luật một-đồng-hồ
        # đã áp cho `_ingested_at`, và start time luôn SỚM HƠN lúc ghi thật nên
        # watermark downstream không bao giờ nhảy qua dòng vừa ghi.
        "processing_run_started_at": to_sql_timestamp(bounds.run_started_at),
    }


def build_command(
    bounds: Bounds,
    *,
    select: str | None = None,
    profiles_dir: str = ".",
) -> list[str]:
    command = [
        os.environ.get("DBT_EXECUTABLE", "dbt"),
        "build",
        "--profiles-dir",
        profiles_dir,
        "--vars",
        json.dumps(build_vars(bounds)),
    ]
    # Cờ dbt thật phải khớp với processing bounds. Chỉ set var=false là
    # đủ cho materialization custom hiện tại, nhưng không đủ cho package/model
    # dùng semantics full-refresh chuẩn của dbt.
    if not bounds.is_incremental:
        command.append("--full-refresh")
    if select:
        command += ["--select", *select.split()]
    return command


def _hydrate_file_backed_settings(environment: dict[str, str]) -> None:
    """Expose Docker secrets only to the dbt child process.

    dbt profile Jinja supports ``env_var`` but cannot read ``*_FILE``. The
    application itself uses file-backed settings, so without this adapter dbt
    silently falls back to the development password declared in profiles.yml.
    File values win over a stale direct variable, matching runtime config.
    """
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


def run_dbt(
    bounds: Bounds,
    *,
    project_dir: Path,
    select: str | None = None,
    runner: Any = subprocess.run,
) -> None:
    """Chạy dbt; exit code khác 0 ném lỗi để runner KHÔNG advance checkpoint."""
    command = build_command(bounds, select=select)
    # Repo được bind-mount vào Airflow với UID khác host. Nếu dbt dùng mặc định
    # ``transform/logs/dbt.log``, một file 0644 do host tạo sẽ làm mọi retry lỗi
    # PermissionError trước cả khi SQL được chạy; compiled artifact trong
    # ``transform/target`` cũng có cùng vấn đề. Log chuẩn vẫn được Airflow thu từ
    # stdout, còn log/artifact tạm đặt ở /tmp để không phụ thuộc owner bind mount.
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
    completed = runner(
        command,
        cwd=project_dir,
        check=False,
        env=environment,
    )
    if completed.returncode != 0:
        raise DbtBuildError(command, completed.returncode)
