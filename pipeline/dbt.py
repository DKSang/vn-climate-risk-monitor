"""Run dbt in-process on the transform/ project."""

from __future__ import annotations

import json
from pathlib import Path

from dbt.cli.main import dbtRunner

PROJECT = Path(__file__).parents[1] / "transform"


def dbt_build(select: str, vars: dict | None = None) -> None:
    """`dbt build` (run + tests) for `select`; raise if any node fails."""
    args = ["build", "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
    args += ["--select", select]
    if vars:
        args += ["--vars", json.dumps(vars, default=str)]
    result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt build --select {select} failed") from result.exception
