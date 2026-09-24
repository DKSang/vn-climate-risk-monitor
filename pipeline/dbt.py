"""Run dbt in-process on the transform/ project."""

from __future__ import annotations

import json
from pathlib import Path

from dbt.cli.main import dbtRunner

PROJECT = Path(__file__).parents[1] / "transform"


def dbt_build(
    select: str | None = None,
    *,
    selector: str | None = None,
    vars: dict | None = None,
) -> None:
    """`dbt build` (seed, run, tests) for a node selection or a named selector."""
    args = ["build", "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    args += ["--selector", selector] if selector else ["--select", select]
    if vars:
        args += ["--vars", json.dumps(vars, default=str)]
    result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(
            f"dbt build {selector or select} failed"
        ) from result.exception
