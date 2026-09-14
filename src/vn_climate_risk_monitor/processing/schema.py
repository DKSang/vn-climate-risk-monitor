"""PostgreSQL DDL cho processing checkpoint và run audit."""

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
        -- Row count sau run để phát hiện target rỗng.
        target_row_count BIGINT,
        -- Snapshot publish sau khi build/test pass.
        published_snapshot_id BIGINT,
        actor TEXT NOT NULL DEFAULT 'runner',
        reason TEXT,
        error_type TEXT,
        error_message TEXT,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    # Mỗi process/scope chỉ có một run đang chạy.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS processing_one_running_per_process
        ON processing.processing_runs (process_key, scope)
        WHERE status = 'RUNNING'
    """,
    """
    CREATE INDEX IF NOT EXISTS processing_runs_history_idx
        ON processing.processing_runs (process_key, scope, started_at_utc DESC)
    """,
    # Migration idempotent cho control plane đang chạy.
    *(
        f"ALTER TABLE processing.processing_runs "
        f"ADD COLUMN IF NOT EXISTS {column} BIGINT"
        for column in (
            "target_row_count",
            "published_snapshot_id",
        )
    ),
)


def ensure_processing_state(connection: DDLConnection) -> None:
    """Create the processing control-plane schema idempotently."""
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
    connection.commit()
