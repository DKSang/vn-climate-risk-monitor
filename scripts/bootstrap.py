#!/usr/bin/env python3
"""
Bootstrap the DuckLake lakehouse.

Idempotent — safe to re-run:
  1. Creates MinIO bucket (skip if exists)
  2. Installs/loads DuckLake extension in DuckDB
  3. Attaches DuckLake catalog (Postgres metadata + MinIO storage)
  4. Creates medallion schemas: bronze, silver, gold

Usage:
    uv run python scripts/bootstrap.py
    # or
    make bootstrap
"""

from __future__ import annotations

import os
import sys
import time

# ---------------------------------------------------------------------------
# Load .env if available
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "vn-climate")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "vnclimate")
PG_USER = os.getenv("POSTGRES_USER", "vnclimate")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "vnclimate")


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
    print(f"  ✗ {name} not reachable at {host}:{port} after {timeout}s", file=sys.stderr)
    sys.exit(1)


def step_1_create_minio_bucket() -> None:
    """Create the MinIO bucket if it doesn't exist."""
    print("\n── Step 1: Create MinIO bucket ──")

    try:
        from minio import Minio
    except ImportError:
        print("  ⚠ minio package not installed, using boto3 fallback...")
        _create_bucket_boto3()
        return

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    if client.bucket_exists(MINIO_BUCKET):
        print(f"  ✓ Bucket '{MINIO_BUCKET}' already exists — skipping")
    else:
        client.make_bucket(MINIO_BUCKET)
        print(f"  ✓ Bucket '{MINIO_BUCKET}' created")


def _create_bucket_boto3() -> None:
    """Fallback: create bucket using boto3."""
    import boto3
    from botocore.exceptions import ClientError

    protocol = "https" if MINIO_SECURE else "http"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"{protocol}://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        print(f"  ✓ Bucket '{MINIO_BUCKET}' already exists — skipping")
    except ClientError:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"  ✓ Bucket '{MINIO_BUCKET}' created")


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
            KEY_ID '{MINIO_ACCESS_KEY}',
            SECRET '{MINIO_SECRET_KEY}',
            ENDPOINT '{MINIO_ENDPOINT}',
            USE_SSL {str(MINIO_SECURE).lower()},
            URL_STYLE 'path'
        );
    """)
    print("  ✓ MinIO secret created")

    # Attach DuckLake catalog
    print("  → Attaching DuckLake catalog (Postgres + MinIO)...")
    pg_conn = (
        f"dbname={PG_DB} host={PG_HOST} port={PG_PORT} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn}' "
        f"AS catalog1 (DATA_PATH 's3://{MINIO_BUCKET}', METADATA_SCHEMA 'ducklake');"
    )
    print("  ✓ DuckLake catalog 'catalog1' attached")

    # Use catalog
    con.execute("USE catalog1;")

    # Create medallion schemas (idempotent)
    print("  → Creating medallion schemas...")
    for schema in ("bronze", "silver", "gold"):
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            print(f"    ✓ Schema '{schema}' ready")
        except duckdb.CatalogException:
            # Schema already exists
            print(f"    ✓ Schema '{schema}' already exists")

    con.close()


def step_3_verify() -> None:
    """Quick verification that catalog tables exist in Postgres."""
    print("\n── Step 3: Quick verification ──")

    try:
        import psycopg2
    except ImportError:
        # Try psycopg (v3)
        try:
            import psycopg

            conn = psycopg.connect(
                host=PG_HOST, port=int(PG_PORT),
                dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'ducklake'
                  AND table_name LIKE 'ducklake_%'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cur.fetchall()]
            conn.close()
        except ImportError:
            # Fallback: verify through DuckDB
            print("  ⚠ No psycopg2/psycopg installed — verifying via DuckDB...")
            _verify_via_duckdb()
            return
    else:
        conn = psycopg2.connect(
            host=PG_HOST, port=int(PG_PORT),
            dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'ducklake'
              AND table_name LIKE 'ducklake_%'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]
        conn.close()

    if tables:
        print(f"  ✓ Found {len(tables)} ducklake_* tables in Postgres:")
        for t in tables:
            print(f"    • {t}")
    else:
        print("  ✗ No ducklake_* tables found — something went wrong!", file=sys.stderr)
        sys.exit(1)


def _verify_via_duckdb() -> None:
    """Verify catalog by reconnecting through DuckDB."""
    import duckdb

    con = duckdb.connect()
    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{MINIO_ACCESS_KEY}',
            SECRET '{MINIO_SECRET_KEY}',
            ENDPOINT '{MINIO_ENDPOINT}',
            USE_SSL {str(MINIO_SECURE).lower()},
            URL_STYLE 'path'
        );
    """)
    pg_conn = (
        f"dbname={PG_DB} host={PG_HOST} port={PG_PORT} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn}' "
        f"AS catalog1 (DATA_PATH 's3://{MINIO_BUCKET}', METADATA_SCHEMA 'ducklake');"
    )
    con.execute("USE catalog1;")

    # Check schemas exist
    schemas = con.sql("SELECT schema_name FROM information_schema.schemata;").fetchall()
    schema_names = [s[0] for s in schemas]
    for expected in ("bronze", "silver", "gold"):
        if expected in schema_names:
            print(f"  ✓ Schema '{expected}' exists")
        else:
            print(f"  ✗ Schema '{expected}' missing!", file=sys.stderr)

    con.close()


def main() -> None:
    print("=" * 60)
    print("  DuckLake Lakehouse Bootstrap")
    print("  Postgres (catalog) + MinIO (storage) + DuckDB (compute)")
    print("=" * 60)

    # Wait for services
    print("\n── Step 0: Waiting for services ──")
    minio_host, minio_port = MINIO_ENDPOINT.split(":")
    _wait_for_service(PG_HOST, int(PG_PORT), "PostgreSQL")
    _wait_for_service(minio_host, int(minio_port), "MinIO")

    step_1_create_minio_bucket()
    step_2_setup_ducklake_catalog()
    step_3_verify()

    print("\n" + "=" * 60)
    print("  ✅ Lakehouse bootstrap complete!")
    print()
    print("  Next steps:")
    print("    uv run python scripts/verify_lakehouse.py  # full POC check")
    print("    make ingest                                 # run dlt pipelines")
    print("=" * 60)


if __name__ == "__main__":
    main()
