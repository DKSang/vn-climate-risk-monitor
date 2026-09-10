"""PostgreSQL DDL for the processing control plane.

Tách HẲN khỏi ``ingestion`` schema. Hai câu hỏi khác nhau, không được trộn:

    ingestion.ingestion_files   File này đã vào Bronze chưa?
    processing.processing_state Process này đã xử lý source tới mốc nào?

Cùng một bảng Bronze có thể nuôi nhiều process với tiến độ hoàn toàn khác nhau,
nên checkpoint thuộc về PROCESS, không thuộc về source.
"""

from __future__ import annotations

from typing import Protocol


class DDLConnection(Protocol):
    """Small DB-API boundary used by bootstrap and tests."""

    def execute(self, query: str, params: object | None = None) -> object: ...

    def commit(self) -> None: ...


SCHEMA_STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS processing",
    """
    CREATE TABLE IF NOT EXISTS processing.processing_state (
        process_key TEXT NOT NULL,
        source_ref TEXT NOT NULL,
        scope TEXT NOT NULL,
        last_successful_start_at TIMESTAMPTZ NOT NULL,
        last_successful_run_id UUID,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (process_key, source_ref, scope)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS processing.processing_runs (
        processing_run_id UUID PRIMARY KEY,
        process_key TEXT NOT NULL,
        scope TEXT NOT NULL,
        target_ref TEXT NOT NULL,
        started_at_utc TIMESTAMPTZ NOT NULL,
        completed_at_utc TIMESTAMPTZ,
        status TEXT NOT NULL
            CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'REWIND')),
        bounds JSONB NOT NULL DEFAULT '{}'::jsonb
            CHECK (jsonb_typeof(bounds) = 'object'),
        checkpoint_candidate TIMESTAMPTZ,
        -- Số dòng trong target SAU khi run thành công. Đủ để bắt "run xanh
        -- nhưng bảng rỗng" — thứ mà insert/update count sinh ra để bắt, nhưng
        -- rẻ hơn nhiều: dbt của ta không phát ra rows_affected cho
        -- materialization DuckLake (main statement là DROP TABLE tmp).
        target_row_count BIGINT,
        -- Snapshot hiện hành sau khi toàn bộ dbt build/test của process pass.
        -- Serving chỉ đọc snapshot của run SUCCEEDED, không đọc HEAD giữa build.
        published_snapshot_id BIGINT,
        -- Soft delete: NULL khi process không khai báo rule nào.
        rows_deactivated BIGINT,
        rows_reactivated BIGINT,
        actor TEXT NOT NULL DEFAULT 'runner',
        reason TEXT,
        error_type TEXT,
        error_message TEXT,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Hai run cùng process ghi đè checkpoint của nhau. Chặn ở DB, không ở code.
    # KHÔNG có lease tự hết hạn: `dbt build` chạy hàng giờ là hợp lệ, một lease
    # đoán sai sẽ giết run đang chạy thật. Run chết phải `abandon` tay, có audit.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS processing_one_running_per_process
        ON processing.processing_runs (process_key, scope)
        WHERE status = 'RUNNING'
    """,
    """
    CREATE INDEX IF NOT EXISTS processing_runs_history_idx
        ON processing.processing_runs (process_key, scope, started_at_utc DESC)
    """,
    # Cột thêm 2026-09-03. ADD COLUMN IF NOT EXISTS để control plane đang chạy
    # nâng cấp tại chỗ, không phải dựng lại.
    *(
        f"ALTER TABLE processing.processing_runs "
        f"ADD COLUMN IF NOT EXISTS {column} BIGINT"
        for column in (
            "target_row_count",
            "published_snapshot_id",
            "rows_deactivated",
            "rows_reactivated",
        )
    ),
)


def ensure_processing_state(connection: DDLConnection) -> None:
    """Create the processing control-plane schema idempotently."""
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
    connection.commit()
