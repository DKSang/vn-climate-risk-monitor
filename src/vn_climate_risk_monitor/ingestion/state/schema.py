"""DDL for file-level incremental ingestion state."""

from __future__ import annotations

import duckdb


def ensure_ingestion_state(connection: duckdb.DuckDBPyConnection) -> None:
    """Create operational schemas and tables idempotently."""
    connection.execute("CREATE SCHEMA IF NOT EXISTS ops")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            run_id VARCHAR NOT NULL,
            pipeline_name VARCHAR NOT NULL,
            scheduled_at_utc TIMESTAMP,
            started_at_utc TIMESTAMP NOT NULL,
            completed_at_utc TIMESTAMP,
            status VARCHAR NOT NULL,
            error_message VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.ingestion_files (
            pipeline_name VARCHAR NOT NULL,
            source_file_path VARCHAR NOT NULL,
            source_file_etag VARCHAR,
            source_file_sha256 VARCHAR NOT NULL,
            source_size_bytes BIGINT NOT NULL,
            source_last_modified TIMESTAMP,
            discovered_at_utc TIMESTAMP NOT NULL,
            processing_started_at_utc TIMESTAMP,
            committed_at_utc TIMESTAMP,
            status VARCHAR NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            rows_parsed BIGINT,
            rows_inserted BIGINT,
            parser_version VARCHAR,
            error_message VARCHAR
        )
        """
    )
