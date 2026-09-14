"""DuckDB connection configured for the DuckLake catalog and MinIO."""

from __future__ import annotations

import os

import duckdb

from vn_climate_risk_monitor.platform.settings import load_settings

PRIMARY_CATALOG = "catalog1"
PRIMARY_METADATA_SCHEMA = "ducklake"

# Raw landing zone; autoloader đọc trực tiếp, không attach qua catalog.
LANDING_PREFIX = "bronze/files/"


def get_connection(
    *,
    catalog_name: str = PRIMARY_CATALOG,
    read_only: bool = False,
    snapshot_version: int | None = None,
) -> duckdb.DuckDBPyConnection:
    """Mở DuckDB connection với MinIO secret và DuckLake catalog."""
    settings = load_settings()
    minio = settings.minio
    postgres = settings.postgres
    con = duckdb.connect()

    # Giảm memory pressure khi transform batch archive lớn.
    con.execute("SET preserve_insertion_order = false;")
    con.execute(f"SET threads = {os.getenv('DUCKDB_THREADS', '4')};")
    con.execute(
        f"SET temp_directory = '{os.getenv('DUCKDB_TEMP_DIR', '/tmp/duckdb_spill')}';"
    )

    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{minio.access_key}',
            SECRET '{minio.secret_key}',
            ENDPOINT '{minio.endpoint}',
            USE_SSL {str(minio.secure).lower()},
            URL_STYLE 'path'
        );
    """)

    attach_options = [
        f"DATA_PATH 's3://{minio.bucket}'",
        f"METADATA_SCHEMA '{PRIMARY_METADATA_SCHEMA}'",
    ]
    if snapshot_version is not None:
        if snapshot_version < 0:
            raise ValueError("snapshot_version phải là số không âm")
        attach_options.append(f"SNAPSHOT_VERSION {snapshot_version}")
    if read_only:
        attach_options.append("READ_ONLY")
    pg_conn_str = postgres.ducklake_connection_string
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn_str}' "
        f"AS {catalog_name} ({', '.join(attach_options)});"
    )

    con.execute(f"USE {catalog_name};")

    return con
