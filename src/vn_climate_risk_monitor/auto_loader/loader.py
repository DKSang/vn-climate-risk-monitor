"""Object discovery and transaction-safe physical loading."""

from __future__ import annotations

import fnmatch
import os
import socket
from collections.abc import Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

ENGINE_VERSION = "1.0.0"
RESERVED_PLACEHOLDERS = frozenset({"files", "ingested_at"})


@dataclass(frozen=True)
class SourceConfig:
    """One code-native source definition plus loader safety limits."""

    name: str
    dataset: str
    prefix: str
    pattern: str
    sql_file: str
    target: str
    batch_size: int
    parameters: Mapping[str, str]
    target_columns: Mapping[str, str] = field(default_factory=dict)
    base_dir: Path = field(default=Path("."))
    scope: str = "production"
    lease_seconds: int = 300
    max_retries: int = 3
    max_batches: int = 100

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError(f"{self.name}: batch_size must be positive")
        for key, value in self.parameters.items():
            if key in RESERVED_PLACEHOLDERS:
                raise ValueError(
                    f"{self.name}: parameter {key!r} trùng placeholder engine"
                )
            if "'" in str(value):
                raise ValueError(
                    f"{self.name}: parameter {key!r} chứa dấu nháy đơn — "
                    "giá trị được chèn thẳng vào SQL nên sẽ làm hỏng câu lệnh"
                )

    @property
    def sql(self) -> str:
        path = self.base_dir / self.sql_file
        if not path.is_file():
            raise FileNotFoundError(f"Không tìm thấy file SQL transform: {path}")
        return path.read_text(encoding="utf-8")


@dataclass(frozen=True)
class DiscoveredObject:
    """An object visible in storage but not yet reconciled with state."""

    object_key: str
    size_bytes: int
    etag: str | None = None
    last_modified: datetime | None = None


class ObjectLister(Protocol):
    def list_objects(
        self, bucket_name: str, prefix: str, recursive: bool
    ) -> Iterator[object]: ...


def discover(
    client: ObjectLister,
    bucket: str,
    prefix: str,
    pattern: str = "**/*.json",
) -> tuple[DiscoveredObject, ...]:
    normalized_prefix = prefix.rstrip("/") + "/"
    found: list[DiscoveredObject] = []
    for entry in client.list_objects(bucket, prefix=normalized_prefix, recursive=True):
        object_key = getattr(entry, "object_name", None)
        if not object_key or object_key.endswith("/"):
            continue
        relative = object_key[len(normalized_prefix) :]
        matches = fnmatch.fnmatch(relative, pattern)
        if pattern.startswith("**/"):
            matches = matches or fnmatch.fnmatch(relative.rsplit("/", 1)[-1], pattern[3:])
        if not matches:
            continue
        found.append(
            DiscoveredObject(
                object_key=object_key,
                size_bytes=getattr(entry, "size", 0) or 0,
                etag=getattr(entry, "etag", None),
                last_modified=getattr(entry, "last_modified", None),
            )
        )
    return tuple(sorted(found, key=lambda item: item.object_key))


def select_new(
    discovered: tuple[DiscoveredObject, ...], already_known: set[str]
) -> tuple[DiscoveredObject, ...]:
    return tuple(item for item in discovered if item.object_key not in already_known)


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
    """Load newly discovered objects into a staging table once per file ledger entry."""

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
            self.config.prefix,
            self.config.pattern,
        )
        known = self.checkpoint.known_object_keys()
        fresh = select_new(found, known)
        if not fresh:
            return len(found), 0

        attempt = self.checkpoint.begin_source_run(
            pipeline_name=self.config.name,
            source_name=self.config.name,
            dataset=self.config.dataset,
            scope=self.config.scope,
            source_uri=f"s3://{self.bucket}/{self.config.prefix}",
            collector_version=ENGINE_VERSION,
            contract_version="1",
            scheduled_at_utc=now,
            expected_file_count=len(fresh),
        )
        try:
            for item in fresh:
                self.checkpoint.register_file(
                    attempt_id=attempt.attempt_id,
                    object_key=item.object_key,
                    file_parameters={"size_bytes": item.size_bytes, "etag": item.etag},
                )
        except BaseException as error:
            self.checkpoint.fail_source_run(
                attempt.attempt_id, completed_at=self.checkpoint.control_now(), error=error
            )
            raise
        self.checkpoint.complete_source_run(
            attempt.attempt_id, completed_at=self.checkpoint.control_now()
        )
        return len(found), len(fresh)

    def claim_batch(self) -> tuple[Any, ...]:
        """Giữ một micro-batch kèm lease. Tương ứng maxFilesPerTrigger."""
        return self.checkpoint.claim_files(
            pipeline_name=self.config.name,
            dataset=self.config.dataset,
            scope=self.config.scope,
            worker_id=self.worker_id,
            limit=self.config.batch_size,
            lease_seconds=self.config.lease_seconds,
            max_retries=self.config.max_retries,
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
        for column, data_type in self.config.target_columns.items():
            quoted_column = '"' + column.replace('"', '""') + '"'
            self.sql.execute(
                f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS "
                f"{quoted_column} {data_type}"
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
        """Atomically replace rows for exactly the claimed source files."""
        select_sql = self._render(uris)
        target = self.config.target
        self._ensure_target(target, select_sql)
        placeholders = ", ".join("?" for _ in uris)
        self.sql.execute("BEGIN TRANSACTION")
        try:
            self.sql.execute(
                f"DELETE FROM {target} WHERE _source_file IN ({placeholders})",
                list(uris),
            )
            result = self.sql.execute(
                f"INSERT INTO {target} BY NAME ({select_sql})"
            ).fetchone()
            self.sql.execute("COMMIT")
        except Exception:
            with suppress(Exception):
                self.sql.execute("ROLLBACK")
            raise
        return int(result[0]) if result else 0

    def load(self, *, now: datetime | None = None) -> LoadResult:
        moment = now or datetime.now(UTC)
        discovered, registered = self.register_new_files(now=moment)

        batches = committed = rows = 0
        failures: list[str] = []
        for _ in range(self.config.max_batches):
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
