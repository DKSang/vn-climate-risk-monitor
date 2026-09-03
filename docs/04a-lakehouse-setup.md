# Setup Lakehouse

> **Đã thay thế một phần (2026-09-03).** Kiến trúc chốt hiện tại ở
> [plan lean medallion](superpowers/plans/2026-09-03-lean-medallion.md) và
> [plan Silver Layer Flow](superpowers/plans/2026-09-03-silver-layer-flow.md):
> MỘT catalog DuckLake (`catalog1`), Bronze chỉ còn là landing zone raw file,
> bảng append-only của autoloader nay là `silver.stg_*`.
> Phần mô tả `bronze_store` / hai catalog / các model gold cũ trong tài liệu
> này KHÔNG còn đúng.

**DuckLake + PostgreSQL catalog + MinIO storage + DuckDB compute**

## Thành phần

```text
DuckDB/dbt
   ├── catalog1 metadata      → PostgreSQL schema ducklake
   ├── ingestion control     → PostgreSQL schema ingestion
   └── Files + Parquet       → s3://vn-climate trên MinIO
```

Bucket chỉ có ba data prefix:

```text
bronze/
silver/
gold/
```

Bronze chỉ có hai prefix con: `bronze/files/` chứa response object nguyên bản do
fetch quản lý; `bronze/tables/` chứa Parquet do DuckLake quản lý. Native
PostgreSQL schema `ingestion` lưu control state và tách khỏi DuckLake catalog.

## Khởi động

```bash
make up
make bootstrap
make quality
```

Kiểm tra sau bootstrap: `make quality` (Provero quét Bronze qua catalog DuckLake,
exit 1 khi có check fail) và `make transform` (dbt build Silver/Gold kèm test).
Script POC `scripts/verify_lakehouse.py` đã xoá — mọi check của nó giờ có bản
chạy liên tục: bootstrap step 4 (schema), dbt build + `assert_gold_is_readable`
(đọc Parquet thật, chống bảng ma), Provero (row_count/freshness).

`make bootstrap` thực hiện idempotently:

1. Tạo bucket MinIO nếu chưa có.
2. Attach catalog chính và catalog Bronze, cùng backed by PostgreSQL.
3. Tạo `catalog1.silver`, `catalog1.silver`, `catalog1.gold`.
4. Tạo `ingestion.ingestion_runs` và `ingestion.ingestion_files` trực tiếp trong PostgreSQL.

## Storage ownership

| Prefix/schema | Owner | Chính sách |
|---|---|---|
| `bronze/files` | fetch | immutable, append-only, không DuckLake cleanup |
| `bronze/tables/<table>` | `catalog1.silver` | snapshot/maintenance qua catalog |
| `silver/<table>` | DuckLake | validated/conformed |
| `gold/<table>` | DuckLake | business-ready |
| `ingestion.*` | ingestion runtime | checkpoint và audit |

Phase 5 đã tạo và vận hành `catalog1.silver.stg_weather_forecast` cho
126 phường/xã; catalog của bảng
nằm trong PostgreSQL `ducklake`, còn Parquet nằm đúng prefix
`bronze/tables/open_meteo_forecast_hourly/`.

Không dùng prefix `raw/` hoặc `landing/`.

## Bảo trì

dbt `on-run-end` expire snapshot cũ hơn bảy ngày và cleanup file không còn được
snapshot tham chiếu. Lệnh này không quản lý object trong `bronze/files`.

```bash
make clean-lake
```

`clean-lake` xóa lịch sử snapshot của cả hai DuckLake catalog; không xóa Bronze files.
