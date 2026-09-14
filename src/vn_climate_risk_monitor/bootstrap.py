"""Bootstrap the DuckLake catalog, schemas, and control-plane tables."""

from __future__ import annotations

import socket
import sys
import time

from vn_climate_risk_monitor.config import Settings, load_settings
from vn_climate_risk_monitor.ingestion.fetch import ensure_bucket
from vn_climate_risk_monitor.ingestion.state import (
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import PRIMARY_CATALOG, PRIMARY_METADATA_SCHEMA
from vn_climate_risk_monitor.processing.schema import ensure_processing_state
from vn_climate_risk_monitor.storage.minio import get_minio_client


def _wait_for_service(host: str, port: int, name: str, timeout: int = 30) -> None:
    """Wait until a TCP port is reachable."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"  ✓ {name} is reachable at {host}:{port}")
                return
        except OSError:
            time.sleep(1)
    print(
        f"  ✗ {name} not reachable at {host}:{port} after {timeout}s",
        file=sys.stderr,
    )
    raise SystemExit(1)


def _create_minio_bucket(settings: Settings) -> None:
    client = get_minio_client(settings.minio)
    if client.bucket_exists(settings.minio.bucket):
        print(f"  ✓ Bucket '{settings.minio.bucket}' already exists — skipping")
    else:
        ensure_bucket(client, settings.minio.bucket)
        print(f"  ✓ Bucket '{settings.minio.bucket}' created")


def _setup_ducklake_catalog(settings: Settings) -> None:
    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("INSTALL ducklake; LOAD ducklake;")
        connection.execute(
            f"""
            CREATE SECRET minio_secret (
                TYPE s3,
                KEY_ID '{settings.minio.access_key}',
                SECRET '{settings.minio.secret_key}',
                ENDPOINT '{settings.minio.endpoint}',
                USE_SSL {str(settings.minio.secure).lower()},
                URL_STYLE 'path'
            );
            """
        )
        connection.execute(
            f"ATTACH 'ducklake:postgres:{settings.postgres.ducklake_connection_string}' "
            f"AS {PRIMARY_CATALOG} (DATA_PATH 's3://{settings.minio.bucket}', "
            f"METADATA_SCHEMA '{PRIMARY_METADATA_SCHEMA}');"
        )
        connection.execute(f"USE {PRIMARY_CATALOG};")
        for schema in ("silver", "gold"):
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    finally:
        connection.close()


def _setup_control_plane(settings: Settings) -> None:
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS airflow")
        ensure_ingestion_state(connection)
        ensure_processing_state(connection)
    finally:
        connection.close()


def _verify(settings: Settings) -> None:
    connection = connect_control_plane(settings.postgres.ducklake_connection_string)
    try:
        rows = connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE (table_schema = 'ingestion' AND table_name IN
                   ('ingestion_runs', 'ingestion_files'))
               OR (table_schema = 'processing' AND table_name IN
                   ('processing_state', 'processing_runs'))
            ORDER BY table_schema, table_name
            """
        ).fetchall()
    finally:
        connection.close()
    required = {
        ("ingestion", "ingestion_runs"),
        ("ingestion", "ingestion_files"),
        ("processing", "processing_state"),
        ("processing", "processing_runs"),
    }
    found = set(rows)
    if missing := required - found:
        print(f"  ✗ Control-plane tables missing: {sorted(missing)}", file=sys.stderr)
        raise SystemExit(1)
    print(f"  ✓ Found {len(rows)} PostgreSQL control tables")


def main() -> None:
    settings = load_settings()
    minio_host, minio_port = settings.minio.endpoint.split(":", 1)
    print("DuckLake Lakehouse Bootstrap")
    _wait_for_service(settings.postgres.host, settings.postgres.port, "PostgreSQL")
    _wait_for_service(minio_host, int(minio_port), "MinIO")
    _create_minio_bucket(settings)
    _setup_ducklake_catalog(settings)
    _setup_control_plane(settings)
    _verify(settings)
    print("Lakehouse bootstrap complete")
