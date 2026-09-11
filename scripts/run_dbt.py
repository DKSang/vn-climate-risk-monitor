#!/usr/bin/env python3
"""Run dbt with file-backed runtime secrets hydrated into the child process."""

from __future__ import annotations

import os
import subprocess
import sys

from processing.dbt import dbt_environment


def main() -> int:
    command = [os.environ.get("DBT_EXECUTABLE", "dbt"), *sys.argv[1:]]
    return subprocess.run(command, check=False, env=dbt_environment()).returncode


if __name__ == "__main__":
    raise SystemExit(main())
