"""PostgreSQL repository for run state and file-level checkpoints."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from autoloader.models import (
    ClaimedObject,
    RunAttempt,
    RunStatus,
    build_logical_run_id,
)

SOURCE_LOGICAL_KEY = "discovery"


class StateTransitionError(RuntimeError):
    """Raised when a state transition does not match the current state."""


def connect_control_plane(dsn: str) -> psycopg.Connection[Any]:
    """Open a direct connection to the PostgreSQL control plane.

    Nhận DSN dạng chuỗi (``postgresql://user:pass@host:port/db`` hoặc keyword
    string của libpq) để package không phụ thuộc kiểu cấu hình của dự án nào.

    ``autocommit=True`` là BẮT BUỘC, không phải tuỳ chọn hiệu năng. Mọi method
    của repository đã tự quản transaction bằng ``with connection.transaction()``.
    Trong psycopg3, block đó chỉ COMMIT khi nó là block ngoài cùng; nếu trước đó
    có một ``execute`` trần mở sẵn transaction ngầm thì nó tụt xuống thành
    SAVEPOINT và không commit gì cả — đóng connection là mất trắng.
    Bật autocommit khiến mỗi block luôn là ngoài cùng, nên luôn commit thật.
    """
    return psycopg.connect(dsn, autocommit=True)


class PostgresIngestionRepository:
    """Keep PostgreSQL as the single source of truth for ingestion metadata."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def ensure_source_run(
        self,
        *,
        pipeline_name: str,
        source_name: str,
        dataset: str,
        scope: str,
        source_uri: str,
        collector_version: str,
        contract_version: str,
        scheduled_at_utc: datetime,
    ) -> RunAttempt:
        """Một logical run SUCCEEDED / nguồn. Load sau chỉ gắn file mới vào đó."""
        identity_values = (
            pipeline_name,
            source_name,
            dataset,
            scope,
            source_uri,
            collector_version,
            contract_version,
        )
        if any(not value.strip() for value in identity_values):
            raise ValueError("Run identity and contract fields must not be empty")
        logical_run_id = build_logical_run_id(
            pipeline_name=pipeline_name,
            dataset=dataset,
            scope=scope,
            logical_key=SOURCE_LOGICAL_KEY,
        )
        with self.connection.transaction():
            self.connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (str(logical_run_id),),
            )
            row = self.connection.execute(
                """
                SELECT attempt_id, logical_run_id, attempt_number, logical_key, status
                FROM ingestion.ingestion_runs
                WHERE logical_run_id = %s AND status = 'SUCCEEDED'
                """,
                (logical_run_id,),
            ).fetchone()
            if row:
                attempt_id, logical_run_id, attempt_number, logical_key, status = row
                return RunAttempt(
                    attempt_id=attempt_id,
                    logical_run_id=logical_run_id,
                    attempt_number=attempt_number,
                    logical_key=logical_key,
                    status=RunStatus(status),
                )
            attempt_id = uuid4()
            # ponytail: expected_file_count CHECK (>0) còn trên schema collector;
            # 1 là dummy. Drop cột khi evolve schema.
            self.connection.execute(
                """
                INSERT INTO ingestion.ingestion_runs (
                    attempt_id, logical_run_id, logical_key, attempt_number,
                    pipeline_name, source_name, dataset, scope,
                    scheduled_at_utc, started_at_utc, completed_at_utc, status,
                    expected_file_count, source_uri, collector_version,
                    contract_version, run_parameters
                ) VALUES (
                    %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s,
                    'SUCCEEDED', 1, %s, %s, %s, '{}'::jsonb
                )
                """,
                (
                    attempt_id,
                    logical_run_id,
                    SOURCE_LOGICAL_KEY,
                    pipeline_name,
                    source_name,
                    dataset,
                    scope,
                    scheduled_at_utc,
                    scheduled_at_utc,
                    scheduled_at_utc,
                    source_uri,
                    collector_version,
                    contract_version,
                ),
            )
        return RunAttempt(
            attempt_id=attempt_id,
            logical_run_id=logical_run_id,
            attempt_number=1,
            logical_key=SOURCE_LOGICAL_KEY,
            status=RunStatus.SUCCEEDED,
        )

    def register_file(
        self,
        *,
        attempt_id: UUID,
        object_key: str,
        file_parameters: Mapping[str, object],
        batch_index: int | None = None,
    ) -> UUID:
        file_id = uuid4()
        with self.connection.transaction():
            if batch_index is None:
                next_index = self.connection.execute(
                    """
                    SELECT COALESCE(MAX(batch_index), -1) + 1
                    FROM ingestion.ingestion_files
                    WHERE attempt_id = %s
                    """,
                    (attempt_id,),
                ).fetchone()
                batch_index = 0 if next_index is None else int(next_index[0])
            cursor = self.connection.execute(
                """
                INSERT INTO ingestion.ingestion_files (
                    file_id, attempt_id, batch_index, object_key,
                    file_parameters, status
                )
                SELECT %s, %s, %s, %s, %s, 'PENDING'
                WHERE EXISTS (
                    SELECT 1 FROM ingestion.ingestion_runs
                    WHERE attempt_id = %s AND status = 'SUCCEEDED'
                )
                """,
                (
                    file_id,
                    attempt_id,
                    batch_index,
                    object_key,
                    Jsonb(dict(file_parameters)),
                    attempt_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "Files can only be added to a SUCCEEDED source run"
                )
        return file_id

    def fail_file(
        self,
        file_id: UUID,
        *,
        error: BaseException,
        worker_id: str | None = None,
    ) -> None:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_files
                SET status = 'FAILED',
                    worker_id = NULL,
                    lease_expires_at_utc = NULL,
                    error_type = %s,
                    error_message = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE file_id = %s
                  AND (
                      (status = 'PENDING' AND %s::text IS NULL)
                      OR (status = 'PROCESSING' AND worker_id = %s)
                  )
                """,
                (
                    type(error).__name__,
                    str(error),
                    file_id,
                    worker_id,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "Only a pending file or its lease owner can fail the file"
                )

    def known_object_keys(self) -> set[str]:
        """Object key đã biết, ở BẤT KỲ trạng thái nào.

        CỐ Ý truy vấn TOÀN CỤC: cột ``object_key`` UNIQUE toàn bảng.
        """
        rows = self.connection.execute(
            "SELECT object_key FROM ingestion.ingestion_files"
        ).fetchall()
        return {row[0] for row in rows}

    def claim_files(
        self,
        *,
        pipeline_name: str,
        dataset: str,
        scope: str,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        max_retries: int,
    ) -> tuple[ClaimedObject, ...]:
        """Claim an available-now micro-batch. Lease gỡ file kẹt PROCESSING."""
        if limit < 1 or lease_seconds < 1 or max_retries < 0:
            raise ValueError("Invalid file claim limits")
        lease = timedelta(seconds=lease_seconds)
        with self.connection.transaction():
            self.connection.execute(
                """
                UPDATE ingestion.ingestion_files
                SET status = 'FAILED',
                    worker_id = NULL,
                    lease_expires_at_utc = NULL,
                    error_type = 'LeaseExpired',
                    error_message = 'Previous processing lease expired',
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE status = 'PROCESSING'
                  AND lease_expires_at_utc < CURRENT_TIMESTAMP
                  AND attempt_id IN (
                      SELECT attempt_id
                      FROM ingestion.ingestion_runs
                      WHERE pipeline_name = %s AND dataset = %s AND scope = %s
                  )
                """,
                (pipeline_name, dataset, scope),
            )
            rows = self.connection.execute(
                """
                WITH candidates AS (
                    SELECT file.file_id
                    FROM ingestion.ingestion_files AS file
                    JOIN ingestion.ingestion_runs AS run
                      ON run.attempt_id = file.attempt_id
                    WHERE run.pipeline_name = %s
                      AND run.dataset = %s
                      AND run.scope = %s
                      AND run.status = 'SUCCEEDED'
                      AND (
                          file.status = 'PENDING'
                          OR (file.status = 'FAILED' AND file.retry_count < %s)
                      )
                    ORDER BY run.scheduled_at_utc, file.batch_index
                    FOR UPDATE OF file SKIP LOCKED
                    LIMIT %s
                )
                UPDATE ingestion.ingestion_files AS file
                SET status = 'PROCESSING',
                    retry_count = file.retry_count
                        + CASE WHEN file.status = 'FAILED' THEN 1 ELSE 0 END,
                    worker_id = %s,
                    processing_started_at_utc = CURRENT_TIMESTAMP,
                    lease_expires_at_utc = CURRENT_TIMESTAMP + %s,
                    error_type = NULL,
                    error_message = NULL,
                    updated_at_utc = CURRENT_TIMESTAMP
                FROM candidates
                WHERE file.file_id = candidates.file_id
                RETURNING file.file_id, file.object_key
                """,
                (
                    pipeline_name,
                    dataset,
                    scope,
                    max_retries,
                    limit,
                    worker_id,
                    lease,
                ),
            ).fetchall()
        return tuple(
            ClaimedObject(file_id=file_id, object_key=object_key)
            for file_id, object_key in rows
        )

    def commit_file(
        self,
        file_id: UUID,
        *,
        worker_id: str,
        committed_at_utc: datetime,
        parser_version: str,
    ) -> None:
        """Đánh dấu file đã nạp xong. Lease phải còn hiệu lực."""
        if not parser_version.strip():
            raise ValueError("parser_version must not be empty")
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_files
                SET status = 'COMMITTED',
                    committed_at_utc = %s,
                    parser_version = %s,
                    worker_id = NULL,
                    lease_expires_at_utc = NULL,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE file_id = %s
                  AND status = 'PROCESSING'
                  AND worker_id = %s
                  AND lease_expires_at_utc >= CURRENT_TIMESTAMP
                """,
                (
                    committed_at_utc,
                    parser_version,
                    file_id,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("Only the lease owner can commit a file")
