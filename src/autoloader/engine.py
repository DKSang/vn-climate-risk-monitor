"""Engine generic: discovery → checkpoint → SQL transform → commit.

Không biết gì về Open-Meteo, thời tiết, hay bất kỳ nguồn cụ thể nào. Mọi thứ
riêng của nguồn nằm trong YAML (:mod:`autoloader.config`) và file SQL.

Vòng đời một lần chạy, bám sát Databricks Auto Loader::

    1. discover()      liệt kê file trên storage        (directory listing)
    2. register        ghi file mới vào checkpoint      (PENDING)
    3. claim_files()   lấy một micro-batch + lease      (maxFilesPerTrigger)
    4. execute SQL     DuckDB đọc file và ghi bảng đích (xử lý bằng SQL)
    5. commit_file()   đánh dấu COMMITTED               (exactly-once)

Bước 4 giao explode/ép kiểu cho DuckDB. Python chỉ điều phối.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from autoloader.config import SourceConfig
from autoloader.discovery import discover, select_new

ENGINE_VERSION = "1.0.0"


class SqlConnection(Protocol):
    """Ranh giới tối thiểu với DuckDB — đủ để test bằng fake."""

    def execute(self, query: str, parameters: object = ...) -> Any: ...


@dataclass(frozen=True)
class LoadResult:
    source: str
    discovered: int
    newly_registered: int
    batches: int
    committed_files: int
    rows_inserted: int
    failures: tuple[str, ...]


class AutoLoader:
    """Nạp file mới từ object storage vào bảng đích, đúng một lần cho mỗi file."""

    def __init__(
        self,
        *,
        config: SourceConfig,
        checkpoint: Any,
        object_client: Any,
        sql: SqlConnection,
        bucket: str,
        worker_id: str | None = None,
    ) -> None:
        self.config = config
        self.checkpoint = checkpoint
        self.object_client = object_client
        self.sql = sql
        self.bucket = bucket
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"

    # ── bước 1 + 2 ────────────────────────────────────────────────────────────
    def register_new_files(self, *, now: datetime) -> tuple[int, int]:
        """Liệt kê storage, ghi file chưa biết vào checkpoint. Trả (thấy, mới)."""
        found = discover(
            self.object_client,
            self.bucket,
            self.config.discovery.prefix,
            self.config.discovery.pattern,
        )
        known = self.checkpoint.known_object_keys()
        fresh = select_new(found, known)
        if not fresh:
            return len(found), 0

        attempt = self.checkpoint.ensure_source_run(
            pipeline_name=self.config.name,
            source_name=self.config.name,
            dataset=self.config.dataset,
            scope=self.config.scope,
            source_uri=f"s3://{self.bucket}/{self.config.discovery.prefix}",
            collector_version=ENGINE_VERSION,
            contract_version="1",
            scheduled_at_utc=now,
        )
        for item in fresh:
            self.checkpoint.register_file(
                attempt_id=attempt.attempt_id,
                object_key=item.object_key,
                file_parameters={"size_bytes": item.size_bytes, "etag": item.etag},
            )
        return len(found), len(fresh)

    # ── bước 3 + 4 + 5 ────────────────────────────────────────────────────────
    def claim_batch(self) -> tuple[Any, ...]:
        """Giữ một micro-batch kèm lease. Tương ứng maxFilesPerTrigger."""
        return self.checkpoint.claim_files(
            pipeline_name=self.config.name,
            dataset=self.config.dataset,
            scope=self.config.scope,
            worker_id=self.worker_id,
            limit=self.config.loader.batch_size,
            lease_seconds=self.config.loader.lease_seconds,
            max_retries=self.config.loader.max_retries,
        )

    def process_batch(
        self, claimed: Sequence[Any] | None = None
    ) -> tuple[int, int, tuple[str, ...], set[Any]]:
        """Chạy SQL cho một lô đã claim rồi commit.

        Trả (số file, số dòng, lỗi, tập file_id đã xử lý).
        """
        if claimed is None:
            claimed = self.claim_batch()
        if not claimed:
            return 0, 0, (), set()

        uris = [f"s3://{self.bucket}/{item.object_key}" for item in claimed]
        try:
            rows = self._run_transform(uris)
        except Exception:  # noqa: BLE001 — cô lập file lỗi ở _isolate_failures
            # KHÔNG đánh hỏng cả lô. Một file JSON hỏng từng làm mất luôn các file
            # lành đi cùng lô: đo 2026-08-21 với batch_size=10, 2 file lành + 1 file
            # hỏng -> 0 dòng vào bảng, cả 3 kẹt FAILED sau khi hết retry.
            # Đó chính là thứ control plane sinh ra để tránh, nên phải cô lập.
            count, isolated_rows, errors = self._isolate_failures(claimed)
            return count, isolated_rows, errors, {i.file_id for i in claimed}

        self._commit_all(claimed)
        return len(claimed), rows, (), {i.file_id for i in claimed}

    def _isolate_failures(
        self, claimed: Sequence[Any]
    ) -> tuple[int, int, tuple[str, ...]]:
        """Chạy lại từng file một để tìm đúng file hỏng.

        Chi phí chỉ phát sinh khi có lỗi; đường thành công vẫn chạy cả lô một lần.
        """
        total_rows = 0
        failures: list[str] = []
        for item in claimed:
            try:
                rows = self._run_transform([f"s3://{self.bucket}/{item.object_key}"])
            except Exception as error:  # noqa: BLE001
                self.checkpoint.fail_file(
                    item.file_id, error=error, worker_id=self.worker_id
                )
                failures.append(
                    f"{item.object_key}: {type(error).__name__}: {error}"
                )
                continue
            self._commit_all([item])
            total_rows += rows
        return len(claimed), total_rows, tuple(failures)

    def _commit_all(self, claimed: Sequence[Any]) -> None:
        # Không ghi rows per-file: INSERT chạy cả lô nên chỉ biết tổng, chia đều
        # là số giả — tổng nằm trong LoadResult (xem commit_file).
        committed_at = datetime.now(UTC)
        for item in claimed:
            self.checkpoint.commit_file(
                item.file_id,
                worker_id=self.worker_id,
                committed_at_utc=committed_at,
                parser_version=ENGINE_VERSION,
            )

    def _run_transform(self, uris: Sequence[str]) -> int:
        """Chạy SQL của nguồn trên đúng danh sách file đã claim."""
        file_list = "[" + ", ".join(f"'{uri}'" for uri in uris) + "]"
        select_sql = self.config.sql.replace("{{ files }}", file_list)
        target = self.config.transform.target
        result = self.sql.execute(
            f"INSERT INTO {target} BY NAME ({select_sql})"
        ).fetchone()
        # DuckDB trả số dòng đã chèn ngay từ câu INSERT
        return int(result[0]) if result else 0

    # ── điều phối ─────────────────────────────────────────────────────────────
    def load(self, *, now: datetime | None = None) -> LoadResult:
        moment = now or datetime.now(UTC)
        discovered, registered = self.register_new_files(now=moment)

        batches = committed = rows = 0
        failures: list[str] = []
        # File đã thử trong LẦN CHẠY NÀY. `claim_files` vẫn trả lại file vừa FAILED
        # khi retry_count chưa chạm trần, nên nếu không nhớ thì vòng lặp sẽ retry
        # ngay lập tức và đốt hết max_batches vào cùng một file hỏng. Retry nên để
        # lần chạy sau — lúc đó nguyên nhân tạm thời (mạng, lock) có thể đã hết.
        for _ in range(self.config.loader.max_batches):
            # KHÔNG lọc file đã thử trong lần chạy này. Từng thử cách đó và nó gây
            # starvation: claim_files sắp xếp theo (scheduled_at, batch_index) nên
            # file hỏng luôn được trả trước, lọc nó ra rồi dừng khiến file lành phía
            # sau không bao giờ tới lượt.
            # `max_retries` đã chặn sẵn: một file hỏng bị claim tối đa max_retries
            # lần rồi bị loại khỏi truy vấn, sau đó lô mới lấy được file lành.
            count, inserted, errors, _ = self.process_batch()
            if count == 0:
                break
            batches += 1
            committed += count
            rows += inserted
            failures.extend(errors)
        return LoadResult(
            source=self.config.name,
            discovered=discovered,
            newly_registered=registered,
            batches=batches,
            committed_files=committed,
            rows_inserted=rows,
            failures=tuple(failures),
        )
