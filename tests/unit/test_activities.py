import shutil
from pathlib import Path

from activities.copy import bento_command, row_env


def test_row_env_uppercases_column_names() -> None:
    assert row_env({"Url": "u"}) == {"ROW_URL": "u"}


def test_bento_command_native_when_bento_on_path(monkeypatch) -> None:
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: "/usr/bin/bento" if name == "bento" else None,
    )
    cmd = bento_command(Path("ingest/copy/x.yaml"), Path("/repo"), {"ROW_URL": "u"})
    assert cmd == ["bento", "-c", "ingest/copy/x.yaml"]


def test_bento_command_docker_passes_only_given_env(monkeypatch) -> None:
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: "/usr/bin/docker" if name == "docker" else None,
    )
    cmd = bento_command(
        Path("ingest/copy/x.yaml"),
        Path("/repo"),
        {"ROW_URL": "u", "MINIO_BUCKET": "vn-climate"},
    )
    env_names = [cmd[i + 1] for i, flag in enumerate(cmd) if flag == "-e"]
    assert env_names == ["ROW_URL", "MINIO_BUCKET"]
