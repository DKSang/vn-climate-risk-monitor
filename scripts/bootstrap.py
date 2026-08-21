#!/usr/bin/env python3
"""
Bootstrap the DuckLake lakehouse.

Idempotent — safe to re-run:
  1. Creates MinIO bucket (skip if exists)
  2. Installs/loads DuckLake extension in DuckDB
  3. Attaches primary and Bronze DuckLake catalogs
  4. Creates ``bronze_store.tables``, ``catalog1.silver`` and ``catalog1.gold``
  5. Creates the native PostgreSQL ingestion control plane

Usage:
    uv run python scripts/bootstrap.py
    # or
    make bootstrap
"""

from __future__ import annotations

import sys
import time

from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.ingestion.state import (
    connect_control_plane,
    ensure_ingestion_state,
)
from vn_climate_risk_monitor.lakehouse import (
    BRONZE_CATALOG,
    BRONZE_METADATA_SCHEMA,
    BRONZE_TABLE_SCHEMA,
    PRIMARY_CATALOG,
    PRIMARY_METADATA_SCHEMA,
)
from vn_climate_risk_monitor.storage import ensure_bucket, get_minio_client

SETTINGS = load_settings()


def _wait_for_service(host: str, port: int, name: str, timeout: int = 30) -> None:
    """Wait until a TCP port is reachable."""
    import socket

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"  ✓ {name} is reachable at {host}:{port}")
                return
        except OSError:
            time.sleep(1)
    print(
        f"  ✗ {name} not reachable at {host}:{port} after {timeout}s", file=sys.stderr
    )
    sys.exit(1)


def step_1_create_minio_bucket() -> None:
    """Create the MinIO bucket if it doesn't exist."""
    print("\n── Step 1: Create MinIO bucket ──")

    client = get_minio_client(SETTINGS.minio)
    if client.bucket_exists(SETTINGS.minio.bucket):
        print(f"  ✓ Bucket '{SETTINGS.minio.bucket}' already exists — skipping")
    else:
        ensure_bucket(client, SETTINGS.minio.bucket)
        print(f"  ✓ Bucket '{SETTINGS.minio.bucket}' created")


def step_2_setup_ducklake_catalog() -> None:
    """Install DuckLake extension and attach catalog to Postgres."""
    print("\n── Step 2: Setup DuckLake catalog ──")

    import duckdb

    con = duckdb.connect()

    # Install and load ducklake extension
    print("  → Installing ducklake extension...")
    con.execute("INSTALL ducklake;")
    con.execute("LOAD ducklake;")
    print("  ✓ ducklake extension ready")

    # Create S3 secret for MinIO
    print("  → Creating MinIO secret...")
    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{SETTINGS.minio.access_key}',
            SECRET '{SETTINGS.minio.secret_key}',
            ENDPOINT '{SETTINGS.minio.endpoint}',
            USE_SSL {str(SETTINGS.minio.secure).lower()},
            URL_STYLE 'path'
        );
    """)
    print("  ✓ MinIO secret created")

    # Attach primary and Bronze DuckLake catalogs. The separate Bronze root is
    # required because DuckLake derives paths as data_path/schema/table.
    print("  → Attaching DuckLake catalogs (Postgres + MinIO)...")
    pg_conn = SETTINGS.postgres.ducklake_connection_string
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn}' "
        f"AS {PRIMARY_CATALOG} (DATA_PATH 's3://{SETTINGS.minio.bucket}', "
        f"METADATA_SCHEMA '{PRIMARY_METADATA_SCHEMA}');"
    )
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn}' "
        f"AS {BRONZE_CATALOG} "
        f"(DATA_PATH 's3://{SETTINGS.minio.bucket}/bronze', "
        f"METADATA_SCHEMA '{BRONZE_METADATA_SCHEMA}');"
    )
    print(f"  ✓ DuckLake catalogs '{PRIMARY_CATALOG}' and '{BRONZE_CATALOG}' attached")

    con.execute(f"USE {PRIMARY_CATALOG};")

    # Create medallion schemas (idempotent). Bronze's logical `tables` schema
    # maps to the physical prefix bronze/tables/.
    print("  → Creating medallion schemas...")
    for schema in ("silver", "gold"):
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            print(f"    ✓ Schema '{schema}' ready")
        except duckdb.CatalogException:
            # Schema already exists
            print(f"    ✓ Schema '{schema}' already exists")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_TABLE_SCHEMA};")
    print(f"    ✓ Schema '{BRONZE_CATALOG}.{BRONZE_TABLE_SCHEMA}' ready")

    con.close()


def step_3_setup_control_plane() -> None:
    """Create ingestion state as native PostgreSQL tables."""
    print("\n── Step 3: Setup ingestion control plane ──")
    connection = connect_control_plane(SETTINGS.postgres)
    try:
        ensure_ingestion_state(connection)
    finally:
        connection.close()
    print("  ✓ ingestion.ingestion_runs and ingestion.ingestion_files ready")


def step_4_verify() -> None:
    """Verify DuckLake metadata and ingestion tables in PostgreSQL."""
    print("\n── Step 4: Quick verification ──")
    connection = connect_control_plane(SETTINGS.postgres)
    try:
        rows = connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE (
                table_schema IN ('ducklake', 'ducklake_bronze')
                AND table_name LIKE 'ducklake_%'
            ) OR (
                table_schema = 'ingestion'
                AND table_name IN ('ingestion_runs', 'ingestion_files')
            )
            ORDER BY table_schema, table_name
            """
        ).fetchall()
    finally:
        connection.close()
    ingestion_tables = {table for schema, table in rows if schema == "ingestion"}
    if ingestion_tables != {"ingestion_runs", "ingestion_files"}:
        print("  ✗ Ingestion control-plane tables missing", file=sys.stderr)
        sys.exit(1)
    print(f"  ✓ Found {len(rows)} PostgreSQL metadata/control tables")


def main() -> None:
    print("=" * 60)
    print("  DuckLake Lakehouse Bootstrap")
    print("  Postgres (catalog) + MinIO (storage) + DuckDB (compute)")
    print("=" * 60)

    # Wait for services
    print("\n── Step 0: Waiting for services ──")
    minio_host, minio_port = SETTINGS.minio.endpoint.split(":")
    _wait_for_service(SETTINGS.postgres.host, SETTINGS.postgres.port, "PostgreSQL")
    _wait_for_service(minio_host, int(minio_port), "MinIO")

    step_1_create_minio_bucket()
    step_2_setup_ducklake_catalog()
    step_3_setup_control_plane()
    step_4_verify()

    print("\n" + "=" * 60)
    print("  ✅ Lakehouse bootstrap complete!")
    print()
    print("  Next steps:")
    print("    uv run python scripts/verify_lakehouse.py  # full POC check")
    print("    make transform                              # build dbt models")
    print("=" * 60)


if __name__ == "__main__":
    main()
