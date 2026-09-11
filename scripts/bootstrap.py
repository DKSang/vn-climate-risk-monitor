#!/usr/bin/env python3
"""Bootstrap the DuckLake catalog, schemas, and control-plane tables."""

from __future__ import annotations

import sys
import time

from autoloader import (
    connect_control_plane,
    ensure_ingestion_state,
)
from processing import ensure_processing_state
from vn_climate_risk_monitor.config import load_settings
from vn_climate_risk_monitor.lakehouse import (
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

    print("  → Installing ducklake extension...")
    con.execute("INSTALL ducklake;")
    con.execute("LOAD ducklake;")
    print("  ✓ ducklake extension ready")

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

    # DuckLake maps schema/table under the configured DATA_PATH.
    print("  → Attaching DuckLake catalog (Postgres + MinIO)...")
    pg_conn = SETTINGS.postgres.ducklake_connection_string
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn}' "
        f"AS {PRIMARY_CATALOG} (DATA_PATH 's3://{SETTINGS.minio.bucket}', "
        f"METADATA_SCHEMA '{PRIMARY_METADATA_SCHEMA}');"
    )
    print(f"  ✓ DuckLake catalog '{PRIMARY_CATALOG}' attached")

    con.execute(f"USE {PRIMARY_CATALOG};")

    # Bronze is raw object storage; Silver and Gold live in DuckLake.
    print("  → Creating medallion schemas...")
    for schema in ("silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        print(f"    ✓ Schema '{schema}' ready")

    con.close()


def step_3_setup_control_plane() -> None:
    """Create ingestion and processing state tables in PostgreSQL."""
    print("\n── Step 3: Setup ingestion + processing control plane ──")
    connection = connect_control_plane(SETTINGS.postgres.ducklake_connection_string)
    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS airflow")
        ensure_ingestion_state(connection)
        ensure_processing_state(connection)
    finally:
        connection.close()
    print("  ✓ ingestion.ingestion_runs, ingestion_files")
    print("  ✓ processing.processing_state, processing_runs")


def step_4_verify() -> None:
    """Verify DuckLake metadata and ingestion tables in PostgreSQL."""
    print("\n── Step 4: Quick verification ──")
    connection = connect_control_plane(SETTINGS.postgres.ducklake_connection_string)
    try:
        rows = connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE (
                table_schema = 'ducklake'
                AND table_name LIKE 'ducklake_%'
            ) OR (
                table_schema = 'ingestion'
                AND table_name IN (
                    'ingestion_runs', 'ingestion_files', 'gold_watermarks'
                )
            ) OR (
                table_schema = 'processing'
                AND table_name IN ('processing_state', 'processing_runs')
            )
            ORDER BY table_schema, table_name
            """
        ).fetchall()
    finally:
        connection.close()
    control_tables = {
        (schema, table)
        for schema, table in rows
        if schema in {"ingestion", "processing"}
    }
    # gold_watermarks is legacy-only and must not be required by bootstrap.
    required = {
        ("ingestion", "ingestion_runs"),
        ("ingestion", "ingestion_files"),
        ("processing", "processing_state"),
        ("processing", "processing_runs"),
    }
    if missing := required - control_tables:
        print(f"  ✗ Control-plane tables missing: {sorted(missing)}", file=sys.stderr)
        sys.exit(1)
    print(f"  ✓ Found {len(rows)} PostgreSQL metadata/control tables")


def main() -> None:
    print("=" * 60)
    print("  DuckLake Lakehouse Bootstrap")
    print("  Postgres (catalog) + MinIO (storage) + DuckDB (compute)")
    print("=" * 60)

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
    print("    docker compose exec -T airflow uv run python scripts/run_processing.py run silver_weather")
    print("    docker compose exec -T airflow uv run python scripts/run_processing.py run rain_gold")
    print("    docker compose exec -T airflow uv run provero run -c quality/provero.yaml --no-optimize --no-store")
    print("=" * 60)


if __name__ == "__main__":
    main()
