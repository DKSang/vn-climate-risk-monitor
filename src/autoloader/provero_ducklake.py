"""Connector DuckLake cho Provero — đọc QUA catalog, không glob Parquet.

── VÌ SAO CẦN ──────────────────────────────────────────────────────────────────
Connector ``duckdb`` mặc định của Provero chỉ gọi ``duckdb.connect(database)``:
không nạp extension, không tạo secret, không ATTACH. Với DuckLake thì nó không
thấy bảng nào, nên cách duy nhất là trỏ ``table`` vào biểu thức
``read_parquet('s3://.../<table>/*.parquet')``.

Cách đó SAI DỮ LIỆU, không chỉ bất tiện. Đã đo 2026-08-21 trên chính dự án này:
sau một lệnh ``DELETE``, catalog trả 10.975 dòng còn glob trả 11.057 — lệch 82
dòng. DuckLake ghi xoá vào metadata chứ không sửa file Parquet, và thư mục bảng
còn chứa file của các snapshot cũ. Glob đọc tất, kể cả dòng đã xoá.

ATTACH cũng không persist được vào file .duckdb (đã thử: mở lại connection là
"Catalog does not exist"), nên không thể lách bằng cách tạo sẵn view.

── CÁCH DÙNG ───────────────────────────────────────────────────────────────────
Đăng ký qua entry_points trong ``pyproject.toml``::

    [project.entry-points."provero.connectors"]
    ducklake = "autoloader.provero_ducklake:DuckLakeConnector"

Rồi trong ``provero.yaml``::

    source:
      type: ducklake
      connection: "ducklake:postgres:dbname=... host=..."
      table: catalog1.silver.stg_weather_forecast

Cấu hình đọc từ biến môi trường (DUCKLAKE_*, MINIO_*) để không lộ secret trong
file YAML. Module này KHÔNG import provero — chỉ dùng duck typing theo đúng
giao diện connector, nên autoloader không phụ thuộc provero.
"""

from __future__ import annotations

import os
import re
from typing import Any

import duckdb

from vn_climate_risk_monitor.config import load_settings


def _sql_literal(value: str) -> str:
    """Quote a trusted configuration value as one DuckDB string literal."""
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"DuckLake alias không hợp lệ: {value!r}")
    return value


class DuckLakeConnection:
    """Bọc connection DuckDB đã ATTACH sẵn DuckLake."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        result = self._conn.execute(query)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def get_columns(self, table: str) -> list[dict[str, Any]]:
        result = self._conn.execute(f"DESCRIBE SELECT * FROM {table}")
        return [
            {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
            for row in result.fetchall()
        ]


class DuckLakeConnector:
    """Connector Provero mở DuckDB rồi ATTACH catalog DuckLake trước khi query."""

    def __init__(
        self,
        connection_string: str | None = None,
        database: str = ":memory:",
        **_: Any,
    ) -> None:
        # Provero gọi plugin bằng keyword `connection_string` (xem
        # provero/connectors/factory.py::create_connector), không phải
        # `connection` như tên trường trong YAML.
        self.database = database
        settings = load_settings()
        # Env override phục vụ container; connection_string giữ compatibility
        # với Provero. Local mặc định dùng cùng typed settings với app.
        self.catalog_dsn = (
            os.getenv("DUCKLAKE_DSN")
            or connection_string
            or f"ducklake:postgres:{settings.postgres.ducklake_connection_string}"
        )
        self.alias = _identifier(os.getenv("DUCKLAKE_ALIAS", "catalog1"))
        self.data_path = os.getenv(
            "DUCKLAKE_DATA_PATH", f"s3://{settings.minio.bucket}"
        )
        self.metadata_schema = os.getenv("DUCKLAKE_METADATA_SCHEMA", "ducklake")
        self.minio = settings.minio

    def connect(self) -> DuckLakeConnection:
        conn = duckdb.connect(
            self.database,
            config={
                "preserve_insertion_order": False,
                "temp_directory": os.getenv("DUCKDB_TEMP_DIR", "/tmp/duckdb_spill"),
                "threads": int(os.getenv("DUCKDB_THREADS", "2")),
            },
        )
        conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake;")
        conn.execute(
            "CREATE OR REPLACE SECRET object_store ("
            "TYPE S3,"
            f" KEY_ID {_sql_literal(self.minio.access_key)},"
            f" SECRET {_sql_literal(self.minio.secret_key)},"
            f" ENDPOINT {_sql_literal(self.minio.endpoint)},"
            f" USE_SSL {'true' if self.minio.secure else 'false'},"
            " URL_STYLE 'path')"
        )
        if not self.catalog_dsn:
            raise ValueError(
                "Thiếu DSN catalog DuckLake: đặt `connection:` trong provero.yaml "
                "hoặc biến môi trường DUCKLAKE_DSN"
            )
        options = (
            [f"DATA_PATH {_sql_literal(self.data_path)}"] if self.data_path else []
        )
        options.append(f"METADATA_SCHEMA {_sql_literal(self.metadata_schema)}")
        conn.execute(
            f"ATTACH IF NOT EXISTS {_sql_literal(self.catalog_dsn)} AS {self.alias} "
            f"({', '.join(options)})"
        )
        return DuckLakeConnection(conn)

    def disconnect(self, connection: DuckLakeConnection) -> None:
        connection._conn.close()

    def get_schema(
        self, connection: DuckLakeConnection, table: str
    ) -> list[dict[str, Any]]:
        return connection.get_columns(table)

    def get_profile(
        self,
        connection: DuckLakeConnection,
        table: str,
        columns: list[str] | None = None,
        sample_size: int | None = None,
    ) -> dict[str, Any]:
        from provero.core.profiler import profile_table

        result = profile_table(connection, table, sample_size=sample_size)
        data: dict[str, Any] = {
            "table": result.table,
            "row_count": result.row_count,
            "column_count": result.column_count,
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "null_count": c.null_count,
                    "null_pct": c.null_pct,
                    "distinct_count": c.distinct_count,
                    "distinct_pct": c.distinct_pct,
                }
                for c in result.columns
            ],
        }
        if columns:
            data["columns"] = [c for c in data["columns"] if c["name"] in columns]
        return data
