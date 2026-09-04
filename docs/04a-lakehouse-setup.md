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
   ├── catalog1 metadata     → PostgreSQL schema ducklake
   ├── ingestion control     → PostgreSQL schema ingestion (file ledger)
   ├── processing control    → PostgreSQL schema processing (watermarks/audit)
   └── Files + Parquet       → s3://vn-climate trên MinIO
```

Bucket có ba data prefix:

```text
bronze/files/   # response object nguyên bản do fetch quản lý
silver/         # Parquet cho staging stg_* và intermediate int_*
gold/           # Parquet cho marts
```

Bronze chỉ có prefix `bronze/files/` chứa response object nguyên bản do
fetch quản lý (không còn `bronze/tables/` — autoloader ghi thẳng vào
`catalog1.silver.stg_*`). PostgreSQL lưu DuckLake catalog metadata trong schema
`ducklake`, đồng thời lưu control plane trong schema `ingestion` (file ledger) và
`processing` (processing state).

## Khởi động

```bash
make up
make bootstrap
make quality
```

Kiểm tra sau bootstrap: `make quality` (Provero quét silver staging qua catalog DuckLake,
exit 1 khi có check fail) và `make transform` (dbt build qua processing framework kèm test).
Script POC `scripts/verify_lakehouse.py` đã xoá — mọi check của nó giờ có bản
chạy liên tục: bootstrap step 4 (schema), dbt build + `assert_gold_is_readable`
(đọc Parquet thật, chống bảng ma), Provero (row_count/freshness).

`make bootstrap` thực hiện idempotently:

1. Tạo bucket MinIO nếu chưa có.
2. Cài/load DuckLake extension trong DuckDB và attach catalog `catalog1` backed by PostgreSQL (`ducklake`).
3. Tạo hai schema `catalog1.silver` và `catalog1.gold`.
4. Tạo control plane tables trong PostgreSQL (schema `ingestion` và `processing`).

## Storage ownership

| Prefix/schema | Owner | Chính sách |
|---|---|---|
| `bronze/files` | fetch | immutable, append-only, không DuckLake cleanup |
| `silver/<table>` | DuckLake | staging append-only (`stg_*`) + intermediate curated (`int_*`) |
| `gold/<table>` | DuckLake | marts business-ready (`dim_*`, `bridge_*`, `fct_*`) |
| `ingestion.*` | ingestion runtime | checkpoint file và discovery |
| `processing.*` | processing runtime | processing checkpoint và audit runs |

Phase 5 đã tạo và vận hành `catalog1.silver.stg_weather_forecast` cho
126 phường/xã; catalog của bảng nằm trong PostgreSQL schema `ducklake`,
còn Parquet nằm đúng prefix `silver/stg_weather_forecast/`.

Không dùng prefix `raw/` hoặc `landing/`.

## Bảo trì

dbt `on-run-end` expire snapshot cũ hơn bảy ngày và cleanup file không còn được
snapshot tham chiếu. Lệnh này không quản lý object trong `bronze/files`.

```bash
make clean-lake
```

`clean-lake` xóa lịch sử snapshot của DuckLake catalog `catalog1`; không xóa Bronze files.
