# Setup Lakehouse

**DuckLake + PostgreSQL catalog + MinIO storage + DuckDB compute**

## Thành phần

```text
DuckDB/dbt
   ├── catalog1 metadata      → PostgreSQL schema ducklake
   ├── bronze_store metadata → PostgreSQL schema ducklake_bronze
   └── Files + Parquet       → s3://vn-climate trên MinIO
```

Bucket chỉ có ba data prefix:

```text
bronze/
silver/
gold/
```

Bronze chỉ có hai prefix con: `bronze/files/` chứa object nguyên bản do collector
quản lý; `bronze/tables/` chứa Parquet do DuckLake quản lý. Schema `ops` nằm
trong catalog chính và chỉ lưu control state.

## Khởi động

```bash
make up
make bootstrap
uv run python scripts/verify_lakehouse.py
```

`make bootstrap` thực hiện idempotently:

1. Tạo bucket MinIO nếu chưa có.
2. Attach catalog chính và catalog Bronze, cùng backed by PostgreSQL.
3. Tạo `bronze_store.tables`, `catalog1.silver`, `catalog1.gold`.
4. Tạo `ops.pipeline_runs` và `ops.ingestion_files`.

## Storage ownership

| Prefix/schema | Owner | Chính sách |
|---|---|---|
| `bronze/files` | collectors | immutable, append-only, không DuckLake cleanup |
| `bronze/tables/<table>` | `bronze_store.tables` | snapshot/maintenance qua catalog |
| `silver/<table>` | DuckLake | validated/conformed |
| `gold/<table>` | DuckLake | business-ready |
| `ops.*` | ingestion runtime | checkpoint và audit |

Không dùng prefix `raw/` hoặc `landing/`.

## Bảo trì

dbt `on-run-end` expire snapshot cũ hơn bảy ngày và cleanup file không còn được
snapshot tham chiếu. Lệnh này không quản lý object trong `bronze/files`.

```bash
make clean-lake
```

`clean-lake` xóa lịch sử snapshot của cả hai DuckLake catalog; không xóa Bronze files.
