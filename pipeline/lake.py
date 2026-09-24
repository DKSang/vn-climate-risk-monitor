"""The lakehouse: DuckLake tables (catalog in Postgres, Parquet on MinIO).

This is the only place that knows how to attach DuckLake from Python.
"""

from __future__ import annotations

import duckdb
from minio import Minio

from pipeline.settings import load_settings

CATALOG = "catalog1"


def connect(*, snapshot: int | None = None) -> duckdb.DuckDBPyConnection:
    """Open DuckDB with the lake attached and selected; pin `snapshot` to read it."""
    settings = load_settings()
    minio = settings.minio
    con = duckdb.connect()
    con.execute(
        f"""
        CREATE SECRET minio (
            TYPE s3, URL_STYLE 'path',
            KEY_ID '{minio.access_key}', SECRET '{minio.secret_key}',
            ENDPOINT '{minio.endpoint}', USE_SSL {str(minio.secure).lower()}
        )
        """
    )
    options = [f"DATA_PATH 's3://{minio.bucket}'", "METADATA_SCHEMA 'ducklake'"]
    if snapshot is not None:
        options += [f"SNAPSHOT_VERSION {int(snapshot)}", "READ_ONLY"]
    con.execute(
        f"ATTACH 'ducklake:postgres:{settings.postgres.dsn}' AS {CATALOG} "
        f"({', '.join(options)})"
    )
    con.execute(f"USE {CATALOG}")
    return con


def minio_client() -> Minio:
    minio = load_settings().minio
    return Minio(
        minio.endpoint,
        access_key=minio.access_key,
        secret_key=minio.secret_key,
        secure=minio.secure,
    )
