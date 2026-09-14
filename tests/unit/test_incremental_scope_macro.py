"""Unit tests for incremental_scope.sql without dbt/DuckDB."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import jinja2
import pytest

MACRO_FILE = Path("transform/macros/incremental_scope.sql")
LOWER = "2026-09-03 09:45:00.000000+00"


def render(
    macro: str,
    *,
    bounds: dict[str, str] | None = None,
    incremental: bool = True,
    **kwargs: Any,
) -> str:
    environment = jinja2.Environment(autoescape=False)
    environment.globals["var"] = lambda name, default=None: {
        "processing_bounds": bounds or {}
    }.get(name, default)
    environment.globals["is_incremental"] = lambda: incremental
    template = environment.from_string(MACRO_FILE.read_text(encoding="utf-8"))
    return str(getattr(template.module, macro)(**kwargs))


def squash(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def input_scope(**overrides: Any) -> str:
    arguments: dict[str, Any] = {
        "relation": "silver.archive_hourly",
        "source_ref": "archive_hourly",
        "dimension": "valid_time_utc",
        "expand_backward": "71 hours",
        "expand_forward": "71 hours",
        "keys": ["grid_cell_id"],
    }
    arguments.update(overrides)
    return squash(
        render("incremental_input_scope", bounds={"archive_hourly": LOWER}, **arguments)
    )


# ── full refresh ─────────────────────────────────────────────────────────────
def test_no_checkpoint_emits_nothing() -> None:
    """Lần chạy đầu phải quét toàn bộ, không được lọc gì."""
    assert (
        squash(
            render(
                "incremental_input_scope",
                bounds={},
                relation="r",
                source_ref="archive_hourly",
                dimension="valid_time_utc",
            )
        )
        == ""
    )


def test_non_incremental_run_emits_nothing() -> None:
    """`--full-refresh` bỏ qua bộ lọc kể cả khi checkpoint đã có."""
    assert input_scope() != ""
    assert (
        squash(
            render(
                "incremental_input_scope",
                bounds={"archive_hourly": LOWER},
                incremental=False,
                relation="r",
                source_ref="archive_hourly",
                dimension="valid_time_utc",
            )
        )
        == ""
    )


def test_bound_of_a_different_source_is_ignored() -> None:
    """Checkpoint tra theo source_ref; nhầm khoá phải thành full refresh, không
    phải im lặng dùng mốc của source khác."""
    assert input_scope(source_ref="ifs_hourly") == ""


# ── hình dạng predicate ──────────────────────────────────────────────────────
def test_input_scope_expands_both_directions() -> None:
    sql = input_scope()

    assert "MIN(valid_time_utc) - INTERVAL '71 hours'" in sql
    assert "MAX(valid_time_utc) + INTERVAL '71 hours'" in sql


def test_input_scope_filters_by_changed_keys() -> None:
    """Không có mệnh đề này thì incremental vẫn quét cả bảng theo chiều key."""
    assert "grid_cell_id IN ( SELECT DISTINCT grid_cell_id" in input_scope()


def test_no_keys_means_no_key_predicate() -> None:
    assert "IN (" not in input_scope(keys=[])


def test_changed_rows_use_platform_timestamp_not_business_time() -> None:
    sql = input_scope()

    assert f"_ingested_at > TIMESTAMPTZ '{LOWER}'" in sql
    assert sql.count("_ingested_at >") == 3  # min, max, keys


def test_change_column_is_overridable() -> None:
    assert "_updated_at >" in input_scope(change_column="_updated_at")


# ── không có upper bound ─────────────────────────────────────────────────────
def test_there_is_no_run_start_upper_bound() -> None:
    """Do not add a run-start upper bound to incremental reads."""
    sql = input_scope()

    assert "<= TIMESTAMPTZ" not in sql
    assert "upper" not in sql.lower()


# ── output scope hẹp hơn input scope ─────────────────────────────────────────
def test_output_scope_does_not_expand_backward() -> None:
    """Output scope starts at the earliest changed business key."""
    sql = squash(
        render(
            "incremental_output_scope",
            bounds={"archive_hourly": LOWER},
            relation="silver.archive_hourly",
            source_ref="archive_hourly",
            dimension="valid_time_utc",
            expand_forward="71 hours",
        )
    )

    assert "MIN(valid_time_utc) FROM" in sql
    assert "- INTERVAL" not in sql
    assert "MAX(valid_time_utc) + INTERVAL '71 hours'" in sql


@pytest.mark.parametrize(
    "macro", ["incremental_input_scope", "incremental_output_scope"]
)
def test_predicate_starts_with_where(macro: str) -> None:
    """Model dán thẳng kết quả sau FROM, nên thiếu WHERE là gãy cú pháp."""
    sql = squash(
        render(
            macro,
            bounds={"archive_hourly": LOWER},
            relation="silver.archive_hourly",
            source_ref="archive_hourly",
            dimension="valid_time_utc",
        )
    )

    assert sql.startswith("WHERE ")


# ── model thật ───────────────────────────────────────────────────────────────
def test_models_declare_lookback_once() -> None:
    """Rolling-window models declare lookback once."""
    for model_path in Path("transform/models").rglob("*.sql"):
        body = model_path.read_text(encoding="utf-8")
        if "incremental_input_scope" not in body:
            continue
        assert not re.search(r"INTERVAL '\d+ hours'", body), model_path
        assert body.count("{% set lookback") == 1, model_path
