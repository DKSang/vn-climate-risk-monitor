# Setup Lakehouse

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

Compose lấy bốn secret bắt buộc từ `.env` rồi mount thành file chỉ đọc trong
`/run/secrets`. PostgreSQL/pgAdmin dùng cơ chế `_FILE` của image; Airflow dùng
`*_CMD`; code dự án ưu tiên `POSTGRES_PASSWORD_FILE` và
`MINIO_SECRET_KEY_FILE`. Không truyền password trực tiếp qua container
environment. Trước deployment ngoài local, thay toàn bộ giá trị `CHANGE_ME` và
không dùng cặp MinIO mặc định.

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

Maintenance tách khỏi dbt build: snapshot kỹ thuật giữ bảy ngày, file Parquet
đã được lên lịch xóa chờ thêm hai ngày. Việc này không xóa lịch sử forecast
đang còn hiệu lực trong table; Raw/Staging forecast cũng được giữ để replay.

```bash
make maintain-lake
```

`clean-lake` xóa lịch sử snapshot của DuckLake catalog `catalog1`; chỉ dùng khi
xử lý emergency sau khi đã backup, không dùng như thao tác dọn disk thường kỳ.
