"""
DuckLake lakehouse connection module.

Provides a reusable DuckDB connection pre-configured with:
  - MinIO secret (S3-compatible storage)
  - DuckLake catalog backed by PostgreSQL
  - Medallion schemas: bronze, silver, gold

Usage:
    from vn_climate_risk_monitor.lakehouse import get_connection

    con = get_connection()
    con.sql("SELECT * FROM gold.risk_hourly LIMIT 5").show()
"""

from __future__ import annotations

import os
from functools import lru_cache

import duckdb


def _env(key: str, default: str = "") -> str:
    """Read an environment variable with fallback."""
    return os.environ.get(key, default)


@lru_cache(maxsize=1)
def _load_dotenv_once() -> None:
    """Load .env file if python-dotenv is available. No-op otherwise."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def get_connection(
    *,
    catalog_name: str = "catalog1",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with DuckLake catalog attached.

    Parameters
    ----------
    catalog_name : str
        Name for the attached DuckLake catalog (default: "catalog1").
    read_only : bool
        If True, attach catalog in read-only mode (for serving layer).

    Returns
    -------
    duckdb.DuckDBPyConnection
        Connection with MinIO secret + DuckLake catalog ready to query.
    """
    _load_dotenv_once()

    # MinIO / S3-compatible config
    minio_endpoint = _env("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key = _env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = _env("MINIO_SECRET_KEY", "minioadmin")
    minio_secure = _env("MINIO_SECURE", "false").lower() == "true"
    minio_bucket = _env("MINIO_BUCKET", "vn-climate")

    # Postgres catalog config
    pg_host = _env("POSTGRES_HOST", "localhost")
    pg_port = _env("POSTGRES_PORT", "5432")
    pg_db = _env("POSTGRES_DB", "vnclimate")
    pg_user = _env("POSTGRES_USER", "vnclimate")
    pg_password = _env("POSTGRES_PASSWORD", "vnclimate")

    con = duckdb.connect()

    # 1) S3 secret for MinIO
    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{minio_access_key}',
            SECRET '{minio_secret_key}',
            ENDPOINT '{minio_endpoint}',
            USE_SSL {str(minio_secure).lower()},
            URL_STYLE 'path'
        );
    """)

    # 2) Attach DuckLake catalog (Postgres metadata + MinIO data)
    attach_opts = (
        f"DATA_PATH 's3://{minio_bucket}', "
        f"METADATA_SCHEMA 'ducklake'"
    )
    if read_only:
        attach_opts += ", READ_ONLY"

    pg_conn_str = (
        f"dbname={pg_db} host={pg_host} port={pg_port} "
        f"user={pg_user} password={pg_password}"
    )
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn_str}' "
        f"AS {catalog_name} ({attach_opts});"
    )

    # 3) Use catalog by default
    con.execute(f"USE {catalog_name};")

    return con
