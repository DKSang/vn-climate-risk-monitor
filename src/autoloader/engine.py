"""Generic file autoloader: discover, claim, transform, commit."""

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


@dataclass(frozen=True)
class _BatchResult:
    claimed_files: int
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
        self._target_ready = False

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

    def process_batch(self, claimed: Sequence[Any] | None = None) -> _BatchResult:
        """Transform một micro-batch và commit các file thành công."""
        if claimed is None:
            claimed = self.claim_batch()
        if not claimed:
            return _BatchResult(0, 0, 0, ())

        uris = [f"s3://{self.bucket}/{item.object_key}" for item in claimed]
        try:
            rows = self._run_transform(uris)
        except Exception:  # noqa: BLE001 - isolate bad files without losing the batch
            return self._isolate_failures(claimed)

        self._commit_all(claimed)
        return _BatchResult(len(claimed), len(claimed), rows, ())

    def _isolate_failures(self, claimed: Sequence[Any]) -> _BatchResult:
        """Retry từng file để cô lập file lỗi."""
        committed_files = 0
        total_rows = 0
        failures: list[str] = []
        for item in claimed:
            try:
                rows = self._run_transform([f"s3://{self.bucket}/{item.object_key}"])
            except Exception as error:  # noqa: BLE001
                self.checkpoint.fail_file(
                    item.file_id, error=error, worker_id=self.worker_id
                )
                failures.append(f"{item.object_key}: {type(error).__name__}: {error}")
                continue
            self._commit_all([item])
            committed_files += 1
            total_rows += rows
        return _BatchResult(
            claimed_files=len(claimed),
            committed_files=committed_files,
            rows_inserted=total_rows,
            failures=tuple(failures),
        )

    def _commit_all(self, claimed: Sequence[Any]) -> None:
        # Batch INSERT chỉ có tổng row count, không gán số giả cho từng file.
        committed_at = datetime.now(UTC)
        for item in claimed:
            self.checkpoint.commit_file(
                item.file_id,
                worker_id=self.worker_id,
                committed_at_utc=committed_at,
                parser_version=ENGINE_VERSION,
            )

    def _ensure_target(self, target: str, select_sql: str) -> None:
        """Tạo bảng đích từ schema của source SQL khi bảng chưa tồn tại."""
        if self._target_ready:
            return
        try:
            self.sql.execute(f"SELECT 1 FROM {target} WHERE false")
        except Exception:  # noqa: BLE001 — chưa có bảng; lỗi khác sẽ nổ ở CREATE
            self.sql.execute(
                f"CREATE TABLE IF NOT EXISTS {target} AS "
                f"SELECT * FROM ({select_sql}) AS shape WHERE false"
            )
        self._target_ready = True

    def _ingested_at_literal(self) -> str:
        """Timestamp `_ingested_at` từ control-plane clock."""
        moment = self.checkpoint.control_now()
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        stamp = moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00")
        return f"TIMESTAMPTZ '{stamp}'"

    def _render(self, uris: Sequence[str]) -> str:
        """Render placeholder engine và source parameters vào SQL."""
        values = {
            "files": "[" + ", ".join(f"'{uri}'" for uri in uris) + "]",
            "ingested_at": self._ingested_at_literal(),
            **self.config.parameters,
        }
        sql = self.config.sql
        for key, value in values.items():
            sql = sql.replace(f"{{{{ {key} }}}}", value)
        return sql

    def _run_transform(self, uris: Sequence[str]) -> int:
        """Chạy SQL của nguồn trên đúng danh sách file đã claim."""
        select_sql = self._render(uris)
        target = self.config.transform.target
        self._ensure_target(target, select_sql)
        result = self.sql.execute(
            f"INSERT INTO {target} BY NAME ({select_sql})"
        ).fetchone()
        return int(result[0]) if result else 0

    def load(self, *, now: datetime | None = None) -> LoadResult:
        moment = now or datetime.now(UTC)
        discovered, registered = self.register_new_files(now=moment)

        batches = committed = rows = 0
        failures: list[str] = []
        for _ in range(self.config.loader.max_batches):
            batch = self.process_batch()
            if batch.claimed_files == 0:
                break
            batches += 1
            committed += batch.committed_files
            rows += batch.rows_inserted
            failures.extend(batch.failures)
        return LoadResult(
            source=self.config.name,
            discovered=discovered,
            newly_registered=registered,
            batches=batches,
            committed_files=committed,
            rows_inserted=rows,
            failures=tuple(failures),
        )
