"""PostgreSQL DDL for the ingestion control plane."""

from __future__ import annotations

from typing import Protocol


class DDLConnection(Protocol):
    """Small DB-API boundary used by bootstrap and tests."""

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
        dataset TEXT NOT NULL,
        scope TEXT NOT NULL,
        scheduled_at_utc TIMESTAMPTZ NOT NULL,
        started_at_utc TIMESTAMPTZ NOT NULL,
        completed_at_utc TIMESTAMPTZ,
        status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
        batch_count INTEGER NOT NULL CHECK (batch_count > 0),
        location_count INTEGER NOT NULL CHECK (location_count > 0),
        source_endpoint TEXT NOT NULL,
        model_requested TEXT NOT NULL,
        forecast_hours INTEGER NOT NULL CHECK (forecast_hours > 0),
        hourly_variables TEXT[] NOT NULL,
        collector_version TEXT NOT NULL,
        request_contract_version INTEGER NOT NULL,
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
        ward_keys BIGINT[] NOT NULL,
        size_bytes BIGINT CHECK (size_bytes >= 0),
        sha256 CHAR(64),
        etag TEXT,
        content_type TEXT,
        http_status INTEGER,
        request_attempt_count INTEGER CHECK (request_attempt_count > 0),
        expected_location_count INTEGER NOT NULL
            CHECK (expected_location_count > 0),
        received_location_count INTEGER CHECK (received_location_count > 0),
        status TEXT NOT NULL
            CHECK (status IN ('PENDING', 'PROCESSING', 'COMMITTED', 'FAILED')),
        retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
        worker_id TEXT,
        processing_started_at_utc TIMESTAMPTZ,
        lease_expires_at_utc TIMESTAMPTZ,
        committed_at_utc TIMESTAMPTZ,
        rows_parsed BIGINT,
        rows_inserted BIGINT,
        rescued_rows BIGINT,
        parser_version TEXT,
        error_type TEXT,
        error_message TEXT,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (attempt_id, batch_index),
        CHECK (cardinality(ward_keys) = expected_location_count)
    )
    """,
    """
    ALTER TABLE ingestion.ingestion_files
        ADD COLUMN IF NOT EXISTS rescued_rows BIGINT
    """,
    """
    CREATE INDEX IF NOT EXISTS ingestion_files_checkpoint_idx
        ON ingestion.ingestion_files (status, created_at_utc)
    """,
)


def ensure_ingestion_state(connection: DDLConnection) -> None:
    """Create the native PostgreSQL control-plane schema idempotently."""
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
    connection.commit()
