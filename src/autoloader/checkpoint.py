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


class StateConflictError(RuntimeError):
    """Raised when a logical run is already active or committed."""


class RunAlreadySucceededError(StateConflictError):
    """Raised when the requested logical schedule slot is already complete."""


class RunAlreadyRunningError(StateConflictError):
    """Raised when another live attempt owns the logical schedule slot."""


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

    def start_run(
        self,
        *,
        pipeline_name: str,
        source_name: str,
        dataset: str,
        scope: str,
        logical_key: str,
        scheduled_at_utc: datetime,
        started_at_utc: datetime,
        expected_file_count: int,
        source_uri: str,
        collector_version: str,
        contract_version: str,
        run_parameters: Mapping[str, object],
        stale_after_seconds: int = 1800,
    ) -> RunAttempt:
        if stale_after_seconds < 1:
            raise ValueError("stale_after_seconds must be positive")
        identity_values = (
            pipeline_name,
            source_name,
            dataset,
            scope,
            logical_key,
            source_uri,
            collector_version,
            contract_version,
        )
        if any(not value.strip() for value in identity_values):
            raise ValueError("Run identity and contract fields must not be empty")
        if expected_file_count < 1:
            raise ValueError("expected_file_count must be positive")
        logical_run_id = build_logical_run_id(
            pipeline_name=pipeline_name,
            dataset=dataset,
            scope=scope,
            logical_key=logical_key,
        )
        attempt_id = uuid4()
        with self.connection.transaction():
            self.connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (str(logical_run_id),),
            )
            self.connection.execute(
                """
                UPDATE ingestion.ingestion_runs
                SET status = 'FAILED',
                    completed_at_utc = %s,
                    error_type = 'CollectorTimeout',
                    error_message = 'Previous collector exceeded its runtime timeout',
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE logical_run_id = %s
                  AND status = 'RUNNING'
                  AND started_at_utc < %s
                """,
                (
                    started_at_utc,
                    logical_run_id,
                    started_at_utc - timedelta(seconds=stale_after_seconds),
                ),
            )
            rows = self.connection.execute(
                """
                SELECT status, attempt_number
                FROM ingestion.ingestion_runs
                WHERE logical_run_id = %s
                ORDER BY attempt_number DESC
                """,
                (logical_run_id,),
            ).fetchall()
            if any(status == RunStatus.SUCCEEDED for status, _ in rows):
                raise RunAlreadySucceededError(
                    f"Logical run already succeeded: {logical_run_id}"
                )
            if any(status == RunStatus.RUNNING for status, _ in rows):
                raise RunAlreadyRunningError(
                    f"Logical run already has a running attempt: {logical_run_id}"
                )
            attempt_number = max((number for _, number in rows), default=0) + 1
            self.connection.execute(
                """
                INSERT INTO ingestion.ingestion_runs (
                    attempt_id, logical_run_id, logical_key, attempt_number,
                    pipeline_name, source_name, dataset, scope,
                    scheduled_at_utc, started_at_utc, status,
                    expected_file_count, source_uri, collector_version,
                    contract_version, run_parameters
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'RUNNING', %s, %s, %s, %s, %s
                )
                """,
                (
                    attempt_id,
                    logical_run_id,
                    logical_key,
                    attempt_number,
                    pipeline_name,
                    source_name,
                    dataset,
                    scope,
                    scheduled_at_utc,
                    started_at_utc,
                    expected_file_count,
                    source_uri,
                    collector_version,
                    contract_version,
                    Jsonb(dict(run_parameters)),
                ),
            )
        return RunAttempt(
            attempt_id=attempt_id,
            logical_run_id=logical_run_id,
            attempt_number=attempt_number,
            logical_key=logical_key,
            status=RunStatus.RUNNING,
        )

    def register_file(
        self,
        *,
        attempt_id: UUID,
        batch_index: int,
        object_key: str,
        expected_item_count: int | None,
        file_parameters: Mapping[str, object],
    ) -> UUID:
        if expected_item_count is not None and expected_item_count < 1:
            raise ValueError("expected_item_count must be positive when provided")
        file_id = uuid4()
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                INSERT INTO ingestion.ingestion_files (
                    file_id, attempt_id, batch_index, object_key,
                    expected_item_count, file_parameters, status
                )
                SELECT %s, %s, %s, %s, %s, %s, 'PENDING'
                WHERE EXISTS (
                    SELECT 1 FROM ingestion.ingestion_runs
                    WHERE attempt_id = %s AND status = 'RUNNING'
                )
                """,
                (
                    file_id,
                    attempt_id,
                    batch_index,
                    object_key,
                    expected_item_count,
                    Jsonb(dict(file_parameters)),
                    attempt_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "Files can only be added to a RUNNING attempt"
                )
        return file_id

    def succeed_run(
        self,
        attempt_id: UUID,
        *,
        completed_at_utc: datetime,
        require_checksums: bool = True,
    ) -> None:
        """Đánh dấu run thành công sau khi mọi file của nó đã sẵn sàng.

        ``require_checksums`` phân biệt hai kiểu run:

        * **Collector run** (mặc định, ``True``): tiến trình tự tải payload về và
          ghi ra storage, nên tính được sha256. Đòi checksum là bằng chứng file
          đã thực sự ghi xong và toàn vẹn trước khi loader được phép đụng vào.
        * **Discovery run** (``False``): engine chỉ LIỆT KÊ file có sẵn trên
          storage, không tải về, nên không có sha256 — và cũng không cần: việc
          file xuất hiện trong listing đã là bằng chứng nó tồn tại. Bắt tính
          checksum ở đây đồng nghĩa tải toàn bộ dữ liệu về chỉ để băm, làm mất
          ý nghĩa của directory listing.
        """
        checksum_clause = (
            "AND file.sha256 IS NOT NULL" if require_checksums else ""
        )
        with self.connection.transaction():
            cursor = self.connection.execute(
                f"""
                UPDATE ingestion.ingestion_runs AS run
                SET status = 'SUCCEEDED',
                    completed_at_utc = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE run.attempt_id = %s
                  AND run.status = 'RUNNING'
                  AND run.expected_file_count = (
                      SELECT count(*) FROM ingestion.ingestion_files AS file
                      WHERE file.attempt_id = run.attempt_id
                        AND file.status = 'PENDING'
                        {checksum_clause}
                        AND (
                            file.expected_item_count IS NULL
                            OR file.received_item_count = file.expected_item_count
                        )
                  )
                """,
                (completed_at_utc, attempt_id),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "A run can only succeed after every batch is recorded and complete"
                )

    def fail_run(
        self,
        attempt_id: UUID,
        *,
        failed_at_utc: datetime,
        error: BaseException,
    ) -> None:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_runs
                SET status = 'FAILED',
                    completed_at_utc = %s,
                    error_type = %s,
                    error_message = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE attempt_id = %s AND status = 'RUNNING'
                """,
                (failed_at_utc, type(error).__name__, str(error), attempt_id),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("Only a RUNNING attempt can fail")

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

    def known_object_keys(
        self,
        *,
        pipeline_name: str,
        dataset: str,
        scope: str,
    ) -> set[str]:
        """Object key đã biết cho một nguồn, ở BẤT KỲ trạng thái nào.

        Đây là mặt đối chiếu của directory listing: file đã có ở đây thì không
        đăng ký lại, dù nó đang PENDING, PROCESSING, COMMITTED hay FAILED.
        Tương ứng checkpoint theo file path của Auto Loader.

        CỐ Ý truy vấn TOÀN CỤC, không lọc theo pipeline/dataset/scope: cột
        ``object_key`` có ràng buộc UNIQUE toàn bảng, nên một file chỉ thuộc về
        đúng một run duy nhất. Nếu lọc theo scope, discovery sẽ tưởng file đã
        đăng ký ở scope khác là mới và cố chèn lại -> vi phạm UNIQUE.
        Tham số vẫn giữ trong chữ ký để tương thích và để mở đường nếu sau này
        ràng buộc đổi thành UNIQUE (pipeline_name, object_key).
        """
        del pipeline_name, dataset, scope  # xem docstring
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
        """Claim an available-now micro-batch without competing worker overlap."""
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
        """Đánh dấu file đã nạp xong.

        KHÔNG ghi rows_parsed/rows_inserted/rescued_rows per-file: engine INSERT
        cả lô bằng một câu SQL nên chỉ biết tổng, chia đều cho từng file là số
        giả. Tổng số dòng của lượt chạy nằm trong LoadResult mà engine in ra.
        Cột metrics trong schema tạm để NULL — dọn ở lần evolve schema tới.
        """
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
