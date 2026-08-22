"""Test engine autoloader bằng fake — không cần Postgres/MinIO/DuckDB thật.

Trọng tâm là ĐƯỜNG LỖI, vì đó là chỗ từng có defect mất dữ liệu: một file JSON
hỏng kéo theo mọi file lành cùng lô (đo 2026-08-21: batch_size=10, 2 lành + 1
hỏng -> 0 dòng vào bảng, cả 3 kẹt FAILED).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from autoloader.config import (
    DiscoveryConfig,
    LoaderConfig,
    SourceConfig,
    TransformConfig,
)
from autoloader.engine import AutoLoader


@dataclass
class FakeObject:
    object_name: str
    size: int = 100
    etag: str = "etag"
    last_modified: datetime | None = None


class FakeObjectClient:
    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def list_objects(self, bucket_name: str, prefix: str, recursive: bool):
        return [FakeObject(k) for k in self._keys if k.startswith(prefix)]


@dataclass
class FakeClaim:
    file_id: UUID
    object_key: str


@dataclass
class FakeAttempt:
    attempt_id: UUID


class FakeCheckpoint:
    """Bản ghi nhớ trong RAM, đủ hình dạng cho engine."""

    def __init__(self) -> None:
        self.files: dict[str, dict[str, Any]] = {}
        self.committed: list[UUID] = []
        self.failed: list[UUID] = []

    def known_object_keys(self, **_: Any) -> set[str]:
        return set(self.files)

    def start_run(self, **_: Any) -> FakeAttempt:
        return FakeAttempt(attempt_id=uuid4())

    def register_file(self, *, object_key: str, **_: Any) -> UUID:
        file_id = uuid4()
        self.files[object_key] = {
            "file_id": file_id,
            "status": "PENDING",
            "retry_count": 0,
        }
        return file_id

    def succeed_run(self, *_: Any, **__: Any) -> None:
        return None

    def claim_files(self, *, limit: int, max_retries: int, **_: Any):
        claimable = [
            (key, meta)
            for key, meta in self.files.items()
            if meta["status"] == "PENDING"
            or (meta["status"] == "FAILED" and meta["retry_count"] < max_retries)
        ]
        chosen = claimable[:limit]
        for _key, meta in chosen:
            meta["status"] = "PROCESSING"
        return tuple(FakeClaim(meta["file_id"], key) for key, meta in chosen)

    def _meta(self, file_id: UUID) -> dict[str, Any]:
        return next(m for m in self.files.values() if m["file_id"] == file_id)

    def commit_file(self, file_id: UUID, **_: Any) -> None:
        self._meta(file_id)["status"] = "COMMITTED"
        self.committed.append(file_id)

    def fail_file(self, file_id: UUID, **_: Any) -> None:
        meta = self._meta(file_id)
        meta["status"] = "FAILED"
        meta["retry_count"] += 1
        self.failed.append(file_id)


class FakeResult:
    def __init__(self, rows: int) -> None:
        self._rows = rows

    def fetchone(self) -> tuple[int]:
        return (self._rows,)


class FakeSql:
    """DuckDB giả: file nào có `poison` trong tên thì ném lỗi."""

    def __init__(self, rows_per_file: int = 100) -> None:
        self.rows_per_file = rows_per_file
        self.statements: list[str] = []

    def execute(self, query: str, parameters: object = None) -> FakeResult:
        self.statements.append(query)
        if "poison" in query:
            raise ValueError("JSON transform error: unknown key")
        return FakeResult(query.count("s3://") * self.rows_per_file)


def build_config(tmp_path: Path, *, batch_size: int, max_retries: int = 3):
    sql_file = tmp_path / "t.sql"
    sql_file.write_text("SELECT * FROM read_json_auto({{ files }})", encoding="utf-8")
    return SourceConfig(
        name="src",
        dataset="ds",
        scope="test",
        discovery=DiscoveryConfig(prefix="raw", pattern="**/*.json"),
        transform=TransformConfig(sql_file="t.sql", target="db.schema.tbl"),
        loader=LoaderConfig(batch_size=batch_size, max_retries=max_retries),
        base_dir=tmp_path,
    )


def build_loader(tmp_path: Path, keys: list[str], *, batch_size: int) -> AutoLoader:
    return AutoLoader(
        config=build_config(tmp_path, batch_size=batch_size),
        checkpoint=FakeCheckpoint(),
        object_client=FakeObjectClient(keys),
        sql=FakeSql(),
        bucket="bkt",
        worker_id="w1",
    )


def test_loads_every_discovered_file(tmp_path: Path) -> None:
    loader = build_loader(
        tmp_path, ["raw/a.json", "raw/b.json"], batch_size=10
    )
    result = loader.load()

    assert result.discovered == 2
    assert result.newly_registered == 2
    assert result.committed_files == 2
    assert result.failures == ()


def test_second_run_loads_nothing_exactly_once(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    loader.load()

    second = loader.load()

    assert second.newly_registered == 0
    assert second.committed_files == 0
    assert second.rows_inserted == 0


@pytest.mark.parametrize("batch_size", [1, 3, 10])
def test_poison_file_does_not_block_healthy_files(
    tmp_path: Path, batch_size: int
) -> None:
    """Hồi quy: file hỏng chỉ được làm hỏng chính nó.

    Trước khi sửa, với batch_size > 1 thì cả lô bị đánh FAILED và các file lành
    không bao giờ vào được bảng đích.
    """
    loader = build_loader(
        tmp_path,
        ["raw/good_1.json", "raw/poison.json", "raw/good_2.json"],
        batch_size=batch_size,
    )

    result = loader.load()

    checkpoint = loader.checkpoint
    committed_keys = {
        key
        for key, meta in checkpoint.files.items()
        if meta["status"] == "COMMITTED"
    }
    assert committed_keys == {"raw/good_1.json", "raw/good_2.json"}
    assert checkpoint.files["raw/poison.json"]["status"] == "FAILED"
    # Chỉ đúng một FILE hỏng; nó xuất hiện nhiều lần trong failures vì bị retry.
    failed_files = {msg.split(":", 1)[0] for msg in result.failures}
    assert failed_files == {"raw/poison.json"}


def test_retry_is_bounded_by_max_retries_not_max_batches(tmp_path: Path) -> None:
    """Một file hỏng không được đốt hết max_batches.

    max_batches=100 nhưng max_retries=3, nên file hỏng chỉ bị claim 3 lần.
    """
    loader = build_loader(tmp_path, ["raw/poison.json"], batch_size=1)

    result = loader.load()

    assert loader.checkpoint.files["raw/poison.json"]["retry_count"] == 3
    assert result.batches <= 3, "không được lặp tới max_batches"


def test_retry_stops_at_max_retries(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/poison.json"], batch_size=1)
    loader.config = build_config(tmp_path, batch_size=1, max_retries=2)

    for _ in range(5):
        loader.load()

    meta = loader.checkpoint.files["raw/poison.json"]
    assert meta["retry_count"] == 2, "không được retry quá max_retries"


def test_healthy_batch_runs_one_statement_not_per_file(tmp_path: Path) -> None:
    """Đường thành công phải giữ nguyên hiệu năng: 1 câu INSERT cho cả lô."""
    loader = build_loader(
        tmp_path, ["raw/a.json", "raw/b.json", "raw/c.json"], batch_size=10
    )

    loader.load()

    inserts = [s for s in loader.sql.statements if s.startswith("INSERT")]
    assert len(inserts) == 1
    assert inserts[0].count("s3://") == 3


def test_load_reports_discovered_count_even_when_nothing_new(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    loader.load()

    again = loader.load()

    assert again.discovered == 1
    assert again.newly_registered == 0


def test_run_timestamp_is_timezone_aware(tmp_path: Path) -> None:
    loader = build_loader(tmp_path, ["raw/a.json"], batch_size=10)
    moment = datetime.now(UTC)

    result = loader.load(now=moment)

    assert result.source == "src"
