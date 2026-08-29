"""
DuckLake lakehouse connection module.

Provides a reusable DuckDB connection pre-configured with:
  - MinIO secret (S3-compatible storage)
  - Primary DuckLake catalog for Silver and Gold
  - Bronze DuckLake catalog rooted at ``bronze/`` with schema ``tables``

Usage:
    from vn_climate_risk_monitor.lakehouse import get_connection

    con = get_connection()
    con.sql("SELECT * FROM gold.risk_hourly LIMIT 5").show()
"""

from __future__ import annotations

import os

import duckdb

from vn_climate_risk_monitor.config import load_settings

PRIMARY_CATALOG = "catalog1"
BRONZE_CATALOG = "bronze_store"
PRIMARY_METADATA_SCHEMA = "ducklake"
BRONZE_METADATA_SCHEMA = "ducklake_bronze"
BRONZE_TABLE_SCHEMA = "tables"


def get_connection(
    *,
    catalog_name: str = PRIMARY_CATALOG,
    bronze_catalog_name: str = BRONZE_CATALOG,
    attach_bronze: bool = True,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with DuckLake catalog attached.

    Parameters
    ----------
    catalog_name : str
        Name for the primary DuckLake catalog.
    bronze_catalog_name : str
        Name for the Bronze DuckLake catalog.
    attach_bronze : bool
        Attach the Bronze catalog. Migration dry-runs can disable this to avoid
        initializing new metadata before execution.
    read_only : bool
        If True, attach catalog in read-only mode (for serving layer).

    Returns
    -------
    duckdb.DuckDBPyConnection
        Connection with MinIO secret + DuckLake catalog ready to query.
    """
    settings = load_settings()
    minio = settings.minio
    postgres = settings.postgres
    con = duckdb.connect()

    # 0) Cấu hình bộ nhớ TRƯỚC mọi thứ khác.
    #
    # Mặc định (12 luồng, preserve_insertion_order=true) làm autoloader OOM khi
    # nạp >= 30 file archive — đo 2026-08-28 trên máy 7GB: 10 file chạy 0,17s,
    # 30 file ném OutOfMemoryException. UNNEST của transform bung một file 600KB
    # thành ~16.000 dòng × 15 cột, và giữ thứ tự chèn buộc phải đệm toàn bộ kết
    # quả đã sắp xếp.
    #
    # Với hai tuỳ chọn dưới: 100 file / 1,54 triệu dòng chạy 0,75s, tuyến tính.
    # Thứ tự chèn không có ý nghĩa ngữ nghĩa ở đây — Silver dedup bằng
    # ROW_NUMBER() với ORDER BY tường minh.
    con.execute("SET preserve_insertion_order = false;")
    con.execute(f"SET threads = {os.getenv('DUCKDB_THREADS', '4')};")
    con.execute(
        f"SET temp_directory = '{os.getenv('DUCKDB_TEMP_DIR', '/tmp/duckdb_spill')}';"
    )

    # 1) S3 secret for MinIO
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

    # 2) Attach two catalogs. DuckLake derives physical paths as
    #    <data_path>/<schema>/<table>. Rooting the Bronze catalog at bronze/
    #    therefore gives the explicit contract bronze/tables/<table>/.
    read_only_option = ", READ_ONLY" if read_only else ""
    pg_conn_str = postgres.ducklake_connection_string
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn_str}' "
        f"AS {catalog_name} (DATA_PATH 's3://{minio.bucket}', "
        f"METADATA_SCHEMA '{PRIMARY_METADATA_SCHEMA}'{read_only_option});"
    )
    if attach_bronze:
        con.execute(
            f"ATTACH 'ducklake:postgres:{pg_conn_str}' "
            f"AS {bronze_catalog_name} (DATA_PATH 's3://{minio.bucket}/bronze', "
            f"METADATA_SCHEMA '{BRONZE_METADATA_SCHEMA}'{read_only_option});"
        )

    # 3) Use catalog by default
    con.execute(f"USE {catalog_name};")

    return con
