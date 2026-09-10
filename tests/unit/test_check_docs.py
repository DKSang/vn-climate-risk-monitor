"""Tests for the lightweight documentation link gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).parents[2] / "scripts" / "check_docs.py"
SPEC = importlib.util.spec_from_file_location("check_docs", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECK_DOCS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK_DOCS)


def test_broken_links_accepts_existing_local_and_external_links(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "target.md").write_text("# Target\n", encoding="utf-8")
    (tmp_path / "README.md").write_text(
        "[local](docs/target.md#section) [web](https://example.com)\n",
        encoding="utf-8",
    )

    assert CHECK_DOCS.broken_links(tmp_path) == []


def test_broken_links_reports_missing_path(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("[missing](docs/nope.md)\n", encoding="utf-8")

    assert CHECK_DOCS.broken_links(tmp_path) == [(Path("README.md"), "docs/nope.md")]
