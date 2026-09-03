"""
DuckLake lakehouse connection module.

MỘT catalog duy nhất cho mọi bảng: silver (staging + curated) và gold.

Trước 2026-09-03 có catalog thứ hai (``bronze_store``, root ``bronze/``) chỉ để
ép đường vật lý thành ``bronze/tables/<table>/``. Nó không còn lý do tồn tại:
Bronze giờ CHỈ là landing zone raw file (``bronze/files/``), còn bảng
append-only mà autoloader ghi đã đúng vai *staging của Silver*
(``silver.stg_*``) chứ không phải một layer riêng.

Usage:
    from vn_climate_risk_monitor.lakehouse import get_connection

    con = get_connection()
    con.sql("SELECT * FROM gold.fct_rain_hourly LIMIT 5").show()
"""

from __future__ import annotations

import os

import duckdb

from vn_climate_risk_monitor.config import load_settings

PRIMARY_CATALOG = "catalog1"
PRIMARY_METADATA_SCHEMA = "ducklake"

# Landing zone raw. Không có catalog nào trỏ vào đây — autoloader đọc file trực
# tiếp bằng read_json_auto. Hằng số này để các script khẳng định "không đụng".
LANDING_PREFIX = "bronze/files/"


def get_connection(
    *,
    catalog_name: str = PRIMARY_CATALOG,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with the DuckLake catalog attached.

    Parameters
    ----------
    catalog_name : str
        Name for the DuckLake catalog.
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

    # 2) Attach MỘT catalog. DuckLake suy đường vật lý là
    #    <data_path>/<schema>/<table>, nên silver.stg_weather_hourly nằm ở
    #    s3://<bucket>/silver/stg_weather_hourly/ — không cần catalog riêng để
    #    điều khiển path như bản hai-catalog trước đây.
    read_only_option = ", READ_ONLY" if read_only else ""
    pg_conn_str = postgres.ducklake_connection_string
    con.execute(
        f"ATTACH 'ducklake:postgres:{pg_conn_str}' "
        f"AS {catalog_name} (DATA_PATH 's3://{minio.bucket}', "
        f"METADATA_SCHEMA '{PRIMARY_METADATA_SCHEMA}'{read_only_option});"
    )

    # 3) Use catalog by default
    con.execute(f"USE {catalog_name};")

    return con
