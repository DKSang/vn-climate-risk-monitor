"""Contract tests for the non-destructive lakehouse backup verifier."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest


def _artifact(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "lakehouse_20260910T000000Z"
    object_file = root / "minio" / "vn-climate" / "gold" / "part.parquet"
    object_file.parent.mkdir(parents=True)
    object_file.write_bytes(b"parquet fixture")
    (root / "postgres").mkdir()
    dump = root / "postgres" / "vnclimate_metadata.dump"
    dump.write_bytes(b"postgres fixture")
    (root / "backup.env").write_text(
        "created_at_utc=20260910T000000Z\nminio_bucket=vn-climate\n",
        encoding="utf-8",
    )
    (root / "minio-manifest.jsonl").write_text(
        '{"status":"success","type":"file","key":"gold/part.parquet"}\n',
        encoding="utf-8",
    )
    object_digest = hashlib.sha256(object_file.read_bytes()).hexdigest()
    (root / "minio-checksums.sha256").write_text(
        f"{object_digest}  minio/vn-climate/gold/part.parquet\n",
        encoding="utf-8",
    )
    dump_digest = hashlib.sha256(dump.read_bytes()).hexdigest()
    dump.with_suffix(".dump.sha256").write_text(
        f"{dump_digest}  {dump.name}\n",
        encoding="utf-8",
    )

    fake_restore = tmp_path / "pg_restore"
    fake_restore.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_restore.chmod(0o700)
    return root, fake_restore


def _verify(root: Path, fake_restore: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        LAKEHOUSE_BACKUP_DIR=str(root),
        PG_RESTORE_BIN=str(fake_restore),
    )
    return subprocess.run(
        ["scripts/verify_lakehouse_backup.sh"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="The backup verifier is a Bash utility; execute it in POSIX CI.",
)
def test_lakehouse_backup_verifier_accepts_complete_artifact(tmp_path: Path) -> None:
    root, fake_restore = _artifact(tmp_path)

    result = _verify(root, fake_restore)

    assert result.returncode == 0, result.stderr
    assert "Backup lakehouse hợp lệ" in result.stdout


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="The backup verifier is a Bash utility; execute it in POSIX CI.",
)
def test_lakehouse_backup_verifier_rejects_corrupt_object(tmp_path: Path) -> None:
    root, fake_restore = _artifact(tmp_path)
    (root / "minio" / "vn-climate" / "gold" / "part.parquet").write_bytes(b"corrupt")

    result = _verify(root, fake_restore)

    assert result.returncode != 0
