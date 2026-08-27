"""Copy Data activity: một row → một lần chạy Bento (GET → object storage).

Mọi cột của row thành biến môi trường ``ROW_<TÊN CỘT>``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path

Row = dict[str, str]

BENTO_IMAGE = "ghcr.io/warpstreamlabs/bento:1.20.0"


def row_env(row: Mapping[str, str]) -> dict[str, str]:
    return {f"ROW_{name.upper()}": value for name, value in row.items()}


def bento_command(config: Path, root: Path, env: Mapping[str, str]) -> list[str]:
    if shutil.which("bento"):
        return ["bento", "-c", str(config)]
    if shutil.which("docker"):
        passthrough = [flag for name in env for flag in ("-e", name)]
        return [
            "docker",
            "run",
            "--rm",
            "--network",
            "host",
            "-v",
            f"{root}:/work",
            "-w",
            "/work",
            *passthrough,
            BENTO_IMAGE,
            "-c",
            str(config),
        ]
    raise RuntimeError(f"Cần `bento` trên PATH hoặc Docker (ảnh {BENTO_IMAGE}).")


def run(
    *,
    config: Path,
    row: Row,
    root: Path,
    env: Mapping[str, str] | None = None,
) -> None:
    extra = dict(env or {}) | row_env(row)
    subprocess.run(
        bento_command(config, root, extra),
        cwd=root,
        env=os.environ | extra,
        check=True,
    )
