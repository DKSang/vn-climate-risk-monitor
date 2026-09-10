"""PostgreSQL repository cho checkpoint và audit của incremental processing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb


class ProcessingStateError(RuntimeError):
    """Raised when a processing state transition does not match the current state."""


def connect_control_plane(dsn: str) -> psycopg.Connection[Any]:
    """Open a direct connection to the PostgreSQL control plane.

    Trùng chức năng với ``autoloader.connect_control_plane`` một cách CÓ CHỦ Ý:
    ``processing`` phải dùng được ở dự án không có autoloader.

    ``autocommit=True`` là BẮT BUỘC. Mọi method dưới đây tự quản transaction bằng
    ``with connection.transaction()``; trong psycopg3 block đó chỉ COMMIT khi nó
    là block ngoài cùng, nếu không thì tụt xuống SAVEPOINT và không commit gì.
    """
    return psycopg.connect(dsn, autocommit=True)


class ProcessingRepository:
    """PostgreSQL là single source of truth cho tiến độ của mỗi process."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    # ── clock ────────────────────────────────────────────────────────────────
    def control_now(self) -> datetime:
        """MỘT đồng hồ duy nhất cho cả platform.

        ``_ingested_at`` của Bronze và ``run_started_at`` của process phải cùng
        nguồn thời gian. Nếu Bronze lấy giờ máy worker còn process lấy giờ máy
        orchestrator thì clock skew vài giây đủ để mất row, và bug đó không tái
        hiện được. Postgres control plane là điểm quy chiếu chung duy nhất mà cả
        hai đều đã kết nối tới.
        """
        row = self.connection.execute("SELECT CURRENT_TIMESTAMP").fetchone()
        assert row is not None
        return row[0]

    # ── checkpoint ───────────────────────────────────────────────────────────
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

    # ── run lifecycle ────────────────────────────────────────────────────────
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
    ) -> UUID:
        """Mở một run RUNNING. Chỉ cho phép một run/process cùng lúc."""
        run_id = uuid4()
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
                        # Candidate chốt ngay từ đầu: checkpoint mới PHẢI là start
                        # time, không phải end time. Row đến trong lúc run chạy sẽ
                        # được lần sau nhặt, thay vì bị nhảy qua.
                        started_at,
                        actor,
                        reason,
                    ),
                )
        except psycopg.errors.UniqueViolation as error:
            raise ProcessingStateError(
                f"{process_key}/{scope}: đã có một run RUNNING. "
                f"Nếu run đó đã chết, chạy `run_processing.py abandon {process_key}`."
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
        """Advance checkpoint — CHỈ gọi sau khi transform + test đã thành công.

        Run status và checkpoint đi cùng MỘT transaction: không có trạng thái
        trung gian mà run là SUCCEEDED nhưng checkpoint chưa nhích, hoặc ngược
        lại (checkpoint nhích mà không ai biết run nào đã nhích nó).
        """
        with self.connection.transaction():
            counts = dict(metrics or {})
            cursor = self.connection.execute(
                """
                UPDATE processing.processing_runs
                SET status = 'SUCCEEDED',
                    completed_at_utc = %s,
                    target_row_count = %s,
                    published_snapshot_id = %s,
                    rows_deactivated = %s,
                    rows_reactivated = %s,
                    updated_at_utc = CURRENT_TIMESTAMP
                WHERE processing_run_id = %s AND status = 'RUNNING'
                """,
                (
                    completed_at,
                    counts.get("target_row_count"),
                    counts.get("published_snapshot_id"),
                    counts.get("rows_deactivated"),
                    counts.get("rows_reactivated"),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ProcessingStateError(
                    f"Run {run_id} không còn ở trạng thái RUNNING"
                )
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

    def fail_run(
        self,
        run_id: UUID,
        *,
        error: BaseException,
        completed_at: datetime,
    ) -> None:
        """Đánh dấu FAILED. KHÔNG chạm processing_state — đó là toàn bộ ý nghĩa
        của failure recovery: lần sau đọc lại đúng cửa sổ vừa hỏng."""
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

    # ── rewind ───────────────────────────────────────────────────────────────
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
        """Kéo checkpoint lùi để reprocess — LUÔN qua đây, không UPDATE tay.

        MERGE idempotent nên reprocess an toàn; thứ không an toàn là không ai
        biết ai đã lùi checkpoint và vì sao. Mỗi lần rewind ghi một row REWIND
        vào processing_runs, nên lịch sử checkpoint đọc được cùng một chỗ với
        lịch sử run.
        """
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
        return run_id

    # ── đọc cho vận hành ─────────────────────────────────────────────────────
    def recent_runs(
        self, *, process_key: str, scope: str, limit: int = 5
    ) -> list[tuple[Any, ...]]:
        return self.connection.execute(
            """
            SELECT processing_run_id, status, started_at_utc, completed_at_utc,
                   checkpoint_candidate, actor, reason, error_type, error_message,
                   target_row_count, published_snapshot_id,
                   rows_deactivated, rows_reactivated
            FROM processing.processing_runs
            WHERE process_key = %s AND scope = %s
            ORDER BY started_at_utc DESC
            LIMIT %s
            """,
            (process_key, scope, limit),
        ).fetchall()
