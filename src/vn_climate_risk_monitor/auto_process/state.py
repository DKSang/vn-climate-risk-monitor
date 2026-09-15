"""PostgreSQL repository cho checkpoint và audit của incremental processing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from vn_climate_risk_monitor.auto_process.config import ACTIVE_PROCESS_KEYS


class ProcessingStateError(RuntimeError):
    """Raised when a processing state transition does not match the current state."""


def ensure_active_process_key(process_key: str) -> None:
    """Reject retired keys on every state-mutating boundary."""
    if process_key not in ACTIVE_PROCESS_KEYS:
        raise ProcessingStateError(
            f"{process_key!r} is not an active processing process key"
        )


def connect_control_plane(dsn: str) -> psycopg.Connection[Any]:
    """Kết nối control plane; mỗi repository method tự quản transaction."""
    return psycopg.connect(dsn, autocommit=True)


class ProcessingRepository:
    """PostgreSQL là single source of truth cho tiến độ của mỗi process."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def control_now(self) -> datetime:
        """Lấy clock chuẩn từ control plane."""
        row = self.connection.execute("SELECT CURRENT_TIMESTAMP").fetchone()
        assert row is not None
        return row[0]

    def read_checkpoints(
        self,
        *,
        process_key: str,
        scope: str,
        source_refs: Sequence[str],
    ) -> dict[str, datetime | None]:
        """Checkpoint của từng source. ``None`` = chưa từng chạy → full refresh."""
        rows = self.connection.execute(
            """
            SELECT source_ref, last_successful_start_at
            FROM processing.processing_state
            WHERE process_key = %s AND scope = %s AND source_ref = ANY(%s)
            """,
            (process_key, scope, list(source_refs)),
        ).fetchall()
        known = {source_ref: value for source_ref, value in rows}
        return {ref: known.get(ref) for ref in source_refs}

    def read_running_run(
        self, *, run_id: UUID, process_key: str, scope: str
    ) -> tuple[UUID, datetime, Mapping[str, Any]]:
        """Return the durable context shared by split Airflow phases."""
        row = self.connection.execute(
            """
            SELECT processing_run_id, checkpoint_candidate, bounds
            FROM processing.processing_runs
            WHERE processing_run_id = %s
              AND process_key = %s AND scope = %s AND status = 'RUNNING'
            """,
            (run_id, process_key, scope),
        ).fetchone()
        if row is None:
            raise ProcessingStateError(
                f"{process_key}/{scope}: không có processing run RUNNING"
            )
        return row

    def read_run_status(
        self, *, run_id: UUID, process_key: str, scope: str
    ) -> str | None:
        row = self.connection.execute(
            """
            SELECT status
            FROM processing.processing_runs
            WHERE processing_run_id = %s AND process_key = %s AND scope = %s
            """,
            (run_id, process_key, scope),
        ).fetchone()
        return None if row is None else str(row[0])

    def _upsert_checkpoints(
        self,
        *,
        process_key: str,
        scope: str,
        source_refs: Sequence[str],
        checkpoint: datetime,
        run_id: UUID,
    ) -> None:
        ensure_active_process_key(process_key)
        for source_ref in source_refs:
            self.connection.execute(
                """
                INSERT INTO processing.processing_state (
                    process_key, source_ref, scope,
                    last_successful_start_at, last_successful_run_id
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (process_key, source_ref, scope) DO UPDATE SET
                    last_successful_start_at = EXCLUDED.last_successful_start_at,
                    last_successful_run_id = EXCLUDED.last_successful_run_id,
                    updated_at_utc = CURRENT_TIMESTAMP
                """,
                (process_key, source_ref, scope, checkpoint, run_id),
            )

    def begin_run(
        self,
        *,
        process_key: str,
        scope: str,
        target_ref: str,
        started_at: datetime,
        bounds: Mapping[str, object],
        actor: str = "runner",
        reason: str | None = None,
        run_id: UUID | None = None,
    ) -> UUID:
        """Mở một run RUNNING. Chỉ cho phép một run/process cùng lúc."""
        ensure_active_process_key(process_key)
        run_id = run_id or uuid4()
        try:
            with self.connection.transaction():
                self.connection.execute(
                    """
                    INSERT INTO processing.processing_runs (
                        processing_run_id, process_key, scope, target_ref,
                        started_at_utc, status, bounds, checkpoint_candidate,
                        actor, reason
                    ) VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        process_key,
                        scope,
                        target_ref,
                        started_at,
                        Jsonb(dict(bounds)),
                        # Chốt theo start time để row đến giữa run được xử lý lần sau.
                        started_at,
                        actor,
                        reason,
                    ),
                )
        except psycopg.errors.UniqueViolation as error:
            raise ProcessingStateError(
                f"{process_key}/{scope}: đã có một run RUNNING. "
                f"Nếu run đó đã chết, chạy `auto-process abandon {process_key}`."
            ) from error
        return run_id

    def complete_run(
        self,
        run_id: UUID,
        *,
        process_key: str,
        scope: str,
        source_refs: Sequence[str],
        checkpoint: datetime,
        completed_at: datetime,
        metrics: Mapping[str, int | None] | None = None,
    ) -> None:
        """Commit run thành công và checkpoint trong cùng transaction."""
        ensure_active_process_key(process_key)
        with self.connection.transaction():
            counts = dict(metrics or {})
            cursor = self.connection.execute(
                """
                UPDATE processing.processing_runs
                SET status = 'SUCCEEDED',
                    completed_at_utc = %s,
                    target_row_count = %s,
                    published_snapshot_id = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE processing_run_id = %s AND status = 'RUNNING'
                """,
                (
                    completed_at,
                    counts.get("target_row_count"),
                    counts.get("published_snapshot_id"),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ProcessingStateError(
                    f"Run {run_id} không còn ở trạng thái RUNNING"
                )
            self._upsert_checkpoints(
                process_key=process_key,
                scope=scope,
                source_refs=source_refs,
                checkpoint=checkpoint,
                run_id=run_id,
            )

    def fail_run(
        self,
        run_id: UUID,
        *,
        process_key: str,
        error: BaseException,
        completed_at: datetime,
    ) -> None:
        """Đánh dấu FAILED, giữ nguyên checkpoint để retry đúng cửa sổ."""
        ensure_active_process_key(process_key)
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE processing.processing_runs
                SET status = 'FAILED',
                    completed_at_utc = %s,
                    error_type = %s,
                    error_message = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE processing_run_id = %s AND status = 'RUNNING'
                """,
                (completed_at, type(error).__name__, str(error), run_id),
            )
            if cursor.rowcount != 1:
                raise ProcessingStateError(
                    f"Run {run_id} không còn ở trạng thái RUNNING"
                )

    def abandon_running(
        self,
        *,
        process_key: str,
        scope: str,
        actor: str,
        reason: str,
        completed_at: datetime,
    ) -> int:
        """Đóng run RUNNING mồ côi (process bị kill). Checkpoint giữ nguyên."""
        ensure_active_process_key(process_key)
        if not reason.strip():
            raise ValueError("abandon cần reason để audit")
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                UPDATE processing.processing_runs
                SET status = 'FAILED',
                    completed_at_utc = %s,
                    error_type = 'Abandoned',
                    error_message = %s,
                    actor = %s,
                    reason = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE process_key = %s AND scope = %s AND status = 'RUNNING'
                """,
                (completed_at, reason, actor, reason, process_key, scope),
            )
        return cursor.rowcount

    def rewind(
        self,
        *,
        process_key: str,
        scope: str,
        target_ref: str,
        source_refs: Sequence[str],
        checkpoint: datetime,
        actor: str,
        reason: str,
        now: datetime,
    ) -> UUID:
        """Kéo checkpoint lùi và ghi audit REWIND."""
        ensure_active_process_key(process_key)
        if not reason.strip():
            raise ValueError("rewind cần reason để audit")
        run_id = uuid4()
        with self.connection.transaction():
            before = dict(
                self.connection.execute(
                    """
                    SELECT source_ref, last_successful_start_at
                    FROM processing.processing_state
                    WHERE process_key = %s AND scope = %s AND source_ref = ANY(%s)
                    """,
                    (process_key, scope, list(source_refs)),
                ).fetchall()
            )
            self.connection.execute(
                """
                INSERT INTO processing.processing_runs (
                    processing_run_id, process_key, scope, target_ref,
                    started_at_utc, completed_at_utc, status,
                    bounds, checkpoint_candidate, actor, reason
                ) VALUES (%s, %s, %s, %s, %s, %s, 'REWIND', %s, %s, %s, %s)
                """,
                (
                    run_id,
                    process_key,
                    scope,
                    target_ref,
                    now,
                    now,
                    Jsonb(
                        {
                            ref: {
                                "checkpoint_before": (
                                    before[ref].isoformat()
                                    if before.get(ref) is not None
                                    else None
                                )
                            }
                            for ref in source_refs
                        }
                    ),
                    checkpoint,
                    actor,
                    reason,
                ),
            )
            self._upsert_checkpoints(
                process_key=process_key,
                scope=scope,
                source_refs=source_refs,
                checkpoint=checkpoint,
                run_id=run_id,
            )
        return run_id

    def recent_runs(
        self, *, process_key: str, scope: str, limit: int = 5
    ) -> list[tuple[Any, ...]]:
        return self.connection.execute(
            """
            SELECT processing_run_id, status, started_at_utc, completed_at_utc,
                   checkpoint_candidate, actor, reason, error_type, error_message,
                   target_row_count, published_snapshot_id
            FROM processing.processing_runs
            WHERE process_key = %s AND scope = %s
            ORDER BY started_at_utc DESC
            LIMIT %s
            """,
            (process_key, scope, limit),
        ).fetchall()
