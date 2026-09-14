"""PostgreSQL repository for run state and file-level checkpoints."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb


class RunStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RunAttempt:
    attempt_id: UUID
    logical_run_id: UUID
    attempt_number: int
    logical_key: str
    status: RunStatus


@dataclass(frozen=True)
class ClaimedObject:
    file_id: UUID
    object_key: str


class DDLConnection(Protocol):
    def execute(self, query: str, params: object | None = None) -> object: ...

    def commit(self) -> None: ...


SCHEMA_STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS ingestion",
    """
    CREATE TABLE IF NOT EXISTS ingestion.ingestion_runs (
        attempt_id UUID PRIMARY KEY,
        logical_run_id UUID NOT NULL,
        logical_key TEXT NOT NULL,
        attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
        pipeline_name TEXT NOT NULL,
        source_name TEXT NOT NULL,
        dataset TEXT NOT NULL,
        scope TEXT NOT NULL,
        scheduled_at_utc TIMESTAMPTZ NOT NULL,
        started_at_utc TIMESTAMPTZ NOT NULL,
        completed_at_utc TIMESTAMPTZ,
        status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
        expected_file_count INTEGER NOT NULL CHECK (expected_file_count > 0),
        source_uri TEXT NOT NULL,
        collector_version TEXT NOT NULL,
        contract_version TEXT NOT NULL,
        run_parameters JSONB NOT NULL DEFAULT '{}'::jsonb
            CHECK (jsonb_typeof(run_parameters) = 'object'),
        error_type TEXT,
        error_message TEXT,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (logical_run_id, attempt_number)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ingestion_one_running_attempt
        ON ingestion.ingestion_runs (logical_run_id)
        WHERE status = 'RUNNING'
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ingestion_one_succeeded_attempt
        ON ingestion.ingestion_runs (logical_run_id)
        WHERE status = 'SUCCEEDED'
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_runs_schedule_idx
        ON ingestion.ingestion_runs (pipeline_name, dataset, scheduled_at_utc)
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_runs_operational_idx
        ON ingestion.ingestion_runs (
            pipeline_name, dataset, scope, scheduled_at_utc DESC
        )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.ingestion_files (
        file_id UUID PRIMARY KEY,
        attempt_id UUID NOT NULL
            REFERENCES ingestion.ingestion_runs (attempt_id),
        batch_index INTEGER NOT NULL CHECK (batch_index >= 0),
        object_key TEXT NOT NULL UNIQUE,
        file_parameters JSONB NOT NULL DEFAULT '{}'::jsonb
            CHECK (jsonb_typeof(file_parameters) = 'object'),
        status TEXT NOT NULL
            CHECK (status IN ('PENDING', 'PROCESSING', 'COMMITTED', 'FAILED')),
        retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
        worker_id TEXT,
        processing_started_at_utc TIMESTAMPTZ,
        lease_expires_at_utc TIMESTAMPTZ,
        committed_at_utc TIMESTAMPTZ,
        parser_version TEXT,
        error_type TEXT,
        error_message TEXT,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (attempt_id, batch_index)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_files_checkpoint_idx
        ON ingestion.ingestion_files (status, created_at_utc)
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.gold_watermarks (
        pipeline_name TEXT PRIMARY KEY,
        last_successful_ingestion_watermark TIMESTAMPTZ NOT NULL,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
)


def ensure_ingestion_state(connection: DDLConnection) -> None:
    """Create the native PostgreSQL control-plane schema idempotently."""
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
    connection.commit()


class StateTransitionError(RuntimeError):
    """Raised when a state transition does not match the current state."""


def connect_control_plane(dsn: str) -> psycopg.Connection[Any]:
    """Kết nối control plane; repository tự quản transaction."""
    return psycopg.connect(dsn, autocommit=True)


class PostgresIngestionRepository:
    """Keep PostgreSQL as the single source of truth for ingestion metadata."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def control_now(self) -> datetime:
        """Clock chuẩn dùng chung cho ingestion và processing."""
        row = self.connection.execute("SELECT CURRENT_TIMESTAMP").fetchone()
        assert row is not None
        return row[0]

    def begin_source_run(
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
        expected_file_count: int,
    ) -> RunAttempt:
        """Start one real discovery run for the files found by this invocation."""
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
        if expected_file_count < 1:
            raise ValueError("expected_file_count must be positive")
        attempt_id = uuid4()
        logical_run_id = attempt_id
        logical_key = scheduled_at_utc.isoformat()
        with self.connection.transaction():
            # ponytail: one Airflow writer; add stale-age fencing before multi-writer use.
            self.connection.execute(
                """
                UPDATE ingestion.ingestion_runs
                SET status = 'FAILED',
                    completed_at_utc = %s,
                    error_type = 'Abandoned',
                    error_message = 'Superseded by the next discovery run',
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE pipeline_name = %s
                  AND dataset = %s
                  AND scope = %s
                  AND status = 'RUNNING'
                """,
                (scheduled_at_utc, pipeline_name, dataset, scope),
            )
            self.connection.execute(
                """
                INSERT INTO ingestion.ingestion_runs (
                    attempt_id, logical_run_id, logical_key, attempt_number,
                    pipeline_name, source_name, dataset, scope,
                    scheduled_at_utc, started_at_utc, status,
                    expected_file_count, source_uri, collector_version,
                    contract_version, run_parameters
                ) VALUES (
                    %s, %s, %s, 1, %s, %s, %s, %s, %s, %s,
                    'RUNNING', %s, %s, %s, %s, '{}'::jsonb
                )
                """,
                (
                    attempt_id,
                    logical_run_id,
                    logical_key,
                    pipeline_name,
                    source_name,
                    dataset,
                    scope,
                    scheduled_at_utc,
                    scheduled_at_utc,
                    expected_file_count,
                    source_uri,
                    collector_version,
                    contract_version,
                ),
            )
        return RunAttempt(
            attempt_id=attempt_id,
            logical_run_id=logical_run_id,
            attempt_number=1,
            logical_key=logical_key,
            status=RunStatus.RUNNING,
        )

    def complete_source_run(self, attempt_id: UUID, *, completed_at: datetime) -> None:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_runs AS run
                SET status = 'SUCCEEDED',
                    completed_at_utc = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE run.attempt_id = %s
                  AND run.status = 'RUNNING'
                  AND run.expected_file_count = (
                      SELECT COUNT(*)
                      FROM ingestion.ingestion_files AS file
                      WHERE file.attempt_id = run.attempt_id
                  )
                """,
                (completed_at, attempt_id),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "Discovery run can complete only after every file is registered"
                )

    def fail_source_run(
        self, attempt_id: UUID, *, completed_at: datetime, error: BaseException
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
                (completed_at, type(error).__name__, str(error), attempt_id),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("Discovery run is not RUNNING")

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
                    WHERE attempt_id = %s AND status = 'RUNNING'
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
                    "Files can only be added to a RUNNING discovery run"
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
        """Object key đã biết; `object_key` unique toàn bảng."""
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
                      AND run.status IN ('SUCCEEDED', 'FAILED')
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
