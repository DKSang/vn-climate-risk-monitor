"""PostgreSQL repository for run state and file-level checkpoints."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

import psycopg

from vn_climate_risk_monitor.config import PostgresSettings, load_settings
from vn_climate_risk_monitor.ingestion.state.models import (
    ClaimedFile,
    PipelineMetrics,
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


class ObjectMetadata(Protocol):
    object_key: str
    size_bytes: int
    sha256: str
    content_type: str
    etag: str | None


def connect_control_plane(
    settings: PostgresSettings | None = None,
) -> psycopg.Connection[Any]:
    """Open a direct connection to the PostgreSQL control plane."""
    config = settings or load_settings().postgres
    return psycopg.connect(**config.connect_kwargs)


class PostgresIngestionRepository:
    """Keep PostgreSQL as the single source of truth for ingestion metadata."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def start_run(
        self,
        *,
        pipeline_name: str,
        dataset: str,
        scope: str,
        logical_key: str,
        scheduled_at_utc: datetime,
        started_at_utc: datetime,
        batch_count: int,
        location_count: int,
        source_endpoint: str,
        model_requested: str,
        forecast_hours: int,
        hourly_variables: Sequence[str],
        collector_version: str,
        request_contract_version: int,
        stale_after_seconds: int = 1800,
    ) -> RunAttempt:
        if stale_after_seconds < 1:
            raise ValueError("stale_after_seconds must be positive")
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
                    pipeline_name, dataset, scope, scheduled_at_utc, started_at_utc,
                    status, batch_count, location_count, source_endpoint,
                    model_requested, forecast_hours, hourly_variables,
                    collector_version, request_contract_version
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'RUNNING', %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    attempt_id,
                    logical_run_id,
                    logical_key,
                    attempt_number,
                    pipeline_name,
                    dataset,
                    scope,
                    scheduled_at_utc,
                    started_at_utc,
                    batch_count,
                    location_count,
                    source_endpoint,
                    model_requested,
                    forecast_hours,
                    list(hourly_variables),
                    collector_version,
                    request_contract_version,
                ),
            )
        return RunAttempt(
            attempt_id=attempt_id,
            logical_run_id=logical_run_id,
            attempt_number=attempt_number,
            logical_key=logical_key,
            status=RunStatus.RUNNING,
        )

    def effective_call_count(
        self,
        *,
        pipeline_name: str,
        since_utc: datetime,
    ) -> int:
        """Count recorded HTTP attempts for a UTC budget window."""
        if since_utc.tzinfo is None or since_utc.utcoffset() is None:
            raise ValueError("since_utc must be timezone-aware")
        with self.connection.transaction():
            row = self.connection.execute(
                """
                SELECT COALESCE(sum(file.request_attempt_count), 0)
                FROM ingestion.ingestion_files AS file
                JOIN ingestion.ingestion_runs AS run USING (attempt_id)
                WHERE run.pipeline_name = %s
                  AND file.created_at_utc >= %s
                """,
                (pipeline_name, since_utc),
            ).fetchone()
        return int(row[0])

    def pipeline_metrics(
        self,
        *,
        pipeline_name: str,
        dataset: str,
        scope: str,
        max_retries: int,
        observed_at_utc: datetime | None = None,
    ) -> PipelineMetrics:
        """Derive health metrics without introducing a third metadata table."""
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")
        observed_at_utc = observed_at_utc or datetime.now(UTC)
        if observed_at_utc.tzinfo is None or observed_at_utc.utcoffset() is None:
            raise ValueError("observed_at_utc must be timezone-aware")
        observed_at_utc = observed_at_utc.astimezone(UTC)
        since_utc = observed_at_utc - timedelta(hours=24)

        with self.connection.transaction():
            latest = self.connection.execute(
                """
                SELECT attempt_id, status, scheduled_at_utc, completed_at_utc
                FROM ingestion.ingestion_runs
                WHERE pipeline_name = %s AND dataset = %s AND scope = %s
                ORDER BY scheduled_at_utc DESC, attempt_number DESC
                LIMIT 1
                """,
                (pipeline_name, dataset, scope),
            ).fetchone()
            run_metrics = self.connection.execute(
                """
                SELECT
                    max(scheduled_at_utc) FILTER (WHERE status = 'SUCCEEDED'),
                    count(*) FILTER (
                        WHERE status = 'SUCCEEDED' AND completed_at_utc >= %s
                    ),
                    count(*) FILTER (
                        WHERE status = 'FAILED' AND completed_at_utc >= %s
                    )
                FROM ingestion.ingestion_runs
                WHERE pipeline_name = %s AND dataset = %s AND scope = %s
                """,
                (since_utc, since_utc, pipeline_name, dataset, scope),
            ).fetchone()
            file_metrics = self.connection.execute(
                """
            SELECT
                count(*) FILTER (WHERE file.status = 'PENDING'),
                count(*) FILTER (WHERE file.status = 'PROCESSING'),
                count(*) FILTER (WHERE file.status = 'FAILED'),
                count(*) FILTER (
                    WHERE file.status = 'FAILED' AND file.retry_count >= %s
                ),
                count(*) FILTER (
                    WHERE file.status = 'PROCESSING'
                      AND file.lease_expires_at_utc < %s
                ),
                count(*) FILTER (
                    WHERE file.status = 'COMMITTED' AND file.committed_at_utc >= %s
                ),
                COALESCE(sum(file.rows_parsed) FILTER (
                    WHERE file.status = 'COMMITTED' AND file.committed_at_utc >= %s
                ), 0),
                COALESCE(sum(file.rows_inserted) FILTER (
                    WHERE file.status = 'COMMITTED' AND file.committed_at_utc >= %s
                ), 0),
                COALESCE(sum(file.rescued_rows) FILTER (
                    WHERE file.status = 'COMMITTED' AND file.committed_at_utc >= %s
                ), 0)
            FROM ingestion.ingestion_files AS file
            JOIN ingestion.ingestion_runs AS run USING (attempt_id)
            WHERE run.pipeline_name = %s
              AND run.dataset = %s
              AND run.scope = %s
              AND run.status = 'SUCCEEDED'
                """,
                (
                    max_retries,
                    observed_at_utc,
                    since_utc,
                    since_utc,
                    since_utc,
                    since_utc,
                    pipeline_name,
                    dataset,
                    scope,
                ),
            ).fetchone()

        return PipelineMetrics(
            pipeline_name=pipeline_name,
            dataset=dataset,
            scope=scope,
            observed_at_utc=observed_at_utc,
            latest_attempt_id=latest[0] if latest else None,
            latest_run_status=RunStatus(latest[1]) if latest else None,
            latest_scheduled_at_utc=latest[2] if latest else None,
            latest_completed_at_utc=latest[3] if latest else None,
            latest_success_at_utc=run_metrics[0],
            succeeded_runs_24h=int(run_metrics[1]),
            failed_runs_24h=int(run_metrics[2]),
            pending_files=int(file_metrics[0]),
            processing_files=int(file_metrics[1]),
            failed_files=int(file_metrics[2]),
            retry_exhausted_files=int(file_metrics[3]),
            expired_leases=int(file_metrics[4]),
            committed_files_24h=int(file_metrics[5]),
            rows_parsed_24h=int(file_metrics[6]),
            rows_inserted_24h=int(file_metrics[7]),
            rescued_rows_24h=int(file_metrics[8]),
        )

    def register_file(
        self,
        *,
        attempt_id: UUID,
        batch_index: int,
        object_key: str,
        ward_keys: Sequence[int],
    ) -> UUID:
        file_id = uuid4()
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                INSERT INTO ingestion.ingestion_files (
                    file_id, attempt_id, batch_index, object_key, ward_keys,
                    expected_location_count, status
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
                    list(ward_keys),
                    len(ward_keys),
                    attempt_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "Files can only be added to a RUNNING attempt"
                )
        return file_id

    def record_file(
        self,
        *,
        file_id: UUID,
        metadata: ObjectMetadata,
        http_status: int,
        request_attempt_count: int,
    ) -> None:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_files
                SET size_bytes = %s,
                    sha256 = %s,
                    etag = %s,
                    content_type = %s,
                    http_status = %s,
                    request_attempt_count = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE file_id = %s
                  AND object_key = %s
                  AND status = 'PENDING'
                  AND sha256 IS NULL
                """,
                (
                    metadata.size_bytes,
                    metadata.sha256,
                    metadata.etag,
                    metadata.content_type,
                    http_status,
                    request_attempt_count,
                    file_id,
                    metadata.object_key,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("Source file metadata was already recorded")

    def validate_file(self, file_id: UUID, *, received_location_count: int) -> None:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_files
                SET received_location_count = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE file_id = %s
                  AND status = 'PENDING'
                  AND sha256 IS NOT NULL
                  AND received_location_count IS NULL
                """,
                (received_location_count, file_id),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError(
                    "Source file validation was already recorded"
                )

    def succeed_run(self, attempt_id: UUID, *, completed_at_utc: datetime) -> None:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_runs AS run
                SET status = 'SUCCEEDED',
                    completed_at_utc = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE run.attempt_id = %s
                  AND run.status = 'RUNNING'
                  AND run.batch_count = (
                      SELECT count(*) FROM ingestion.ingestion_files AS file
                      WHERE file.attempt_id = run.attempt_id
                        AND file.status = 'PENDING'
                        AND file.sha256 IS NOT NULL
                        AND file.received_location_count = file.expected_location_count
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
    ) -> tuple[ClaimedFile, ...]:
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
                    SELECT file.file_id, run.logical_run_id,
                           run.scheduled_at_utc, run.started_at_utc,
                           run.completed_at_utc, run.source_endpoint,
                           run.model_requested, run.forecast_hours,
                           run.hourly_variables, run.collector_version,
                           run.request_contract_version
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
                RETURNING file.file_id, file.attempt_id, file.object_key,
                          file.batch_index, file.ward_keys, file.size_bytes,
                          file.sha256, file.content_type, file.retry_count,
                          file.expected_location_count,
                          file.received_location_count,
                          candidates.logical_run_id,
                          candidates.scheduled_at_utc,
                          candidates.started_at_utc,
                          candidates.completed_at_utc,
                          candidates.source_endpoint,
                          candidates.model_requested,
                          candidates.forecast_hours,
                          candidates.hourly_variables,
                          candidates.collector_version,
                          candidates.request_contract_version
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
            ClaimedFile(
                file_id=file_id,
                attempt_id=attempt_id,
                logical_run_id=logical_run_id,
                object_key=object_key,
                batch_index=batch_index,
                ward_keys=tuple(ward_keys),
                size_bytes=size_bytes,
                sha256=sha256,
                content_type=content_type,
                retry_count=retry_count,
                scheduled_at_utc=scheduled_at_utc,
                collection_started_at_utc=collection_started_at_utc,
                collection_completed_at_utc=collection_completed_at_utc,
                source_endpoint=source_endpoint,
                model_requested=model_requested,
                forecast_hours=forecast_hours,
                hourly_variables=tuple(hourly_variables),
                collector_version=collector_version,
                request_contract_version=request_contract_version,
                expected_location_count=expected_location_count,
                received_location_count=received_location_count,
            )
            for (
                file_id,
                attempt_id,
                object_key,
                batch_index,
                ward_keys,
                size_bytes,
                sha256,
                content_type,
                retry_count,
                expected_location_count,
                received_location_count,
                logical_run_id,
                scheduled_at_utc,
                collection_started_at_utc,
                collection_completed_at_utc,
                source_endpoint,
                model_requested,
                forecast_hours,
                hourly_variables,
                collector_version,
                request_contract_version,
            ) in rows
        )

    def commit_file(
        self,
        file_id: UUID,
        *,
        worker_id: str,
        committed_at_utc: datetime,
        rows_parsed: int,
        rows_inserted: int,
        rescued_rows: int,
        parser_version: str,
    ) -> None:
        if min(rows_parsed, rows_inserted, rescued_rows) < 0:
            raise ValueError("File row metrics must not be negative")
        if rows_inserted > rows_parsed or rescued_rows > rows_parsed:
            raise ValueError("File row metrics exceed rows parsed")
        if not parser_version.strip():
            raise ValueError("parser_version must not be empty")
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE ingestion.ingestion_files
                SET status = 'COMMITTED',
                    committed_at_utc = %s,
                    rows_parsed = %s,
                    rows_inserted = %s,
                    rescued_rows = %s,
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
                    rows_parsed,
                    rows_inserted,
                    rescued_rows,
                    parser_version,
                    file_id,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateTransitionError("Only the lease owner can commit a file")
