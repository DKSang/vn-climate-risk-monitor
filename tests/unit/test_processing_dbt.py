"""Runner dbt: Bounds → --vars, và exit code → không advance checkpoint."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from processing.dbt import (
    DbtBuildError,
    build_command,
    build_vars,
    run_dbt,
    to_sql_timestamp,
)
from processing.runner import Bounds, SourceBounds


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 3, hour, minute, tzinfo=UTC)


def bounds(*, lower: datetime | None) -> Bounds:
    return Bounds(
        run_started_at=at(11, 0),
        sources=(
            SourceBounds(
                source_ref="archive_hourly",
                change_column="_ingested_at",
                checkpoint_before=lower + timedelta(minutes=15) if lower else None,
                lower_bound=lower,
            ),
        ),
    )


def test_timestamp_literal_is_engine_parseable() -> None:
    """Không dùng isoformat(): 'T' + '+00:00' không phải text format của DuckDB."""
    literal = to_sql_timestamp(at(9, 45))

    assert literal == "2026-09-03 09:45:00.000000+00"
    assert "T" not in literal


def test_naive_timestamp_is_treated_as_utc() -> None:
    naive = datetime(2026, 9, 3, 9, 45)  # noqa: DTZ001 — chính là input cần test
    assert to_sql_timestamp(naive) == to_sql_timestamp(at(9, 45))


def test_incremental_vars_carry_lower_bound() -> None:
    variables = build_vars(bounds(lower=at(9, 45)))

    assert variables["processing_incremental"] is True
    assert variables["processing_bounds"]["archive_hourly"].startswith(
        "2026-09-03 09:45:00"
    )


def test_vars_carry_run_start_for_updated_at() -> None:
    """`_updated_at` của lớp mutable phải dùng đồng hồ Postgres, không phải DuckDB.

    Bơm xuống ngay cả khi full refresh: model vẫn cần đóng dấu dòng nó ghi.
    """
    incremental = build_vars(bounds(lower=at(9, 45)))
    full = build_vars(bounds(lower=None))

    assert incremental["processing_run_started_at"].startswith("2026-09-03 11:00:00")
    assert full["processing_run_started_at"].startswith("2026-09-03 11:00:00")


def test_full_refresh_vars_omit_bounds() -> None:
    variables = build_vars(bounds(lower=None))

    assert variables["processing_incremental"] is False
    assert variables["processing_bounds"] == {}


def test_command_passes_vars_as_json_and_select() -> None:
    command = build_command(bounds(lower=at(9, 45)), select="+tag:historical")

    assert command[1] == "build"
    assert command[command.index("--indirect-selection") + 1] == "cautious"
    payload = json.loads(command[command.index("--vars") + 1])
    assert payload["processing_incremental"] is True
    assert command[command.index("--select") + 1] == "+tag:historical"


def test_command_without_select_builds_whole_project() -> None:
    command = build_command(bounds(lower=None))

    assert "--select" not in command
    assert "--full-refresh" in command


def test_incremental_command_does_not_pass_full_refresh() -> None:
    assert "--full-refresh" not in build_command(bounds(lower=at(9, 45)))


class _Completed:
    def __init__(self, returncode: int) -> None:
        self.returncode = returncode


def test_nonzero_exit_raises_so_checkpoint_never_advances() -> None:
    """Toàn bộ failure recovery dựa vào việc lỗi này được ném ra."""
    with pytest.raises(DbtBuildError):
        run_dbt(
            bounds(lower=at(9, 45)),
            project_dir=Path("transform"),
            runner=lambda *a, **k: _Completed(1),
        )


def test_zero_exit_returns_quietly() -> None:
    calls: list[dict[str, Any]] = []

    def fake(command: list[str], **kwargs: Any) -> _Completed:
        calls.append({"command": command, **kwargs})
        return _Completed(0)

    run_dbt(bounds(lower=at(9, 45)), project_dir=Path("transform"), runner=fake)

    assert calls[0]["cwd"] == Path("transform")
    assert calls[0]["env"]["DBT_LOG_PATH"].endswith("vn-climate-dbt-logs")
    assert calls[0]["env"]["DBT_TARGET_PATH"].endswith("vn-climate-dbt-target")


def test_file_backed_secrets_are_forwarded_only_to_dbt_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    postgres = tmp_path / "postgres_password"
    minio = tmp_path / "minio_secret_key"
    postgres.write_text("postgres-from-file\n", encoding="utf-8")
    minio.write_text("minio-from-file\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_PASSWORD", "stale-environment-value")
    monkeypatch.setenv("POSTGRES_PASSWORD_FILE", str(postgres))
    monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
    monkeypatch.setenv("MINIO_SECRET_KEY_FILE", str(minio))
    calls: list[dict[str, Any]] = []

    def fake(command: list[str], **kwargs: Any) -> _Completed:
        calls.append({"command": command, **kwargs})
        return _Completed(0)

    run_dbt(bounds(lower=at(9, 45)), project_dir=Path("transform"), runner=fake)

    child_environment = calls[0]["env"]
    assert child_environment["POSTGRES_PASSWORD"] == "postgres-from-file"
    assert child_environment["MINIO_SECRET_KEY"] == "minio-from-file"
    assert os.environ["POSTGRES_PASSWORD"] == "stale-environment-value"
