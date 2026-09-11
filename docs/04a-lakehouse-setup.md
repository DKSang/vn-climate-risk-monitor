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
docker compose up -d --build
```

Compose tự sinh secret local ở lần chạy đầu và giữ chúng trong named volume
`runtime_secrets`. PostgreSQL/pgAdmin dùng cơ chế `_FILE`; Airflow dùng `*_CMD`;
code dự án đọc `POSTGRES_PASSWORD_FILE` và `MINIO_SECRET_KEY_FILE`. `.env` chỉ
cần khi muốn override cấu hình hoặc cung cấp credential cố định ngay từ lần chạy
đầu.

Service `bootstrap` chạy sau khi PostgreSQL và MinIO healthy, tạo bucket,
DuckLake/control plane và geography seed trước khi Airflow hoặc dashboard khởi
động. Có thể chạy lại idempotently bằng `docker compose run --rm bootstrap`.

Các check runtime chạy trong Airflow container để dùng đúng secret và dependency
đã khóa. Bootstrap kiểm tra schema; dbt có `assert_gold_is_readable` để buộc đọc
Parquet thật; Provero kiểm tra row count/freshness.

Bootstrap thực hiện idempotently:

1. Tạo bucket MinIO nếu chưa có.
2. Cài/load DuckLake extension trong DuckDB và attach catalog `catalog1` backed by PostgreSQL (`ducklake`).
3. Tạo hai schema `catalog1.silver` và `catalog1.gold`.
4. Tạo control plane tables trong PostgreSQL (schema `ingestion` và `processing`).
5. Seed geography và build các model tĩnh cần trước khi DAG chạy.

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
docker compose exec -T airflow uv run python scripts/maintain_lake.py \
  --snapshot-retention-days 7 --file-grace-days 2
```

`clean-lake` xóa lịch sử snapshot của DuckLake catalog `catalog1`; chỉ dùng khi
xử lý emergency sau khi đã backup, không dùng như thao tác dọn disk thường kỳ.
