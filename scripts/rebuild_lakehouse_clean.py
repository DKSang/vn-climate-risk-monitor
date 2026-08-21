#!/usr/bin/env python3
"""Rebuild dbt-managed models without deleting immutable Bronze files.

The former implementation deleted the entire ``bronze/`` prefix. That is no
longer valid because ``bronze/files`` is the immutable replay source.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    result = subprocess.run(
        ["uv", "run", "dbt", "build", "--profiles-dir", "."],
        cwd=PROJECT_ROOT / "transform",
        check=False,
    )
    if result.returncode:
        raise SystemExit(result.returncode)
    print("Rebuilt dbt-managed Bronze, Silver and Gold models.")
    print("Immutable objects under bronze/files were not modified.")


if __name__ == "__main__":
    sys.exit(main())
