# Cấu trúc repository

**Hanoi Flood & Climate Risk Monitor** · v2.2 · 2026-09-08

Repository dùng MinIO landing zone (`bronze/files/`) và hai schema medallion
trong DuckLake (`silver`, `gold`). Cấu trúc dbt tổ chức thành 3 lớp chuẩn:
`staging → intermediate → marts`. PostgreSQL schema `ingestion` và `processing`
là native control plane cho file ledger và processing checkpoint.

## Cây thư mục

```text
vn-climate-risk-monitor/
├── src/fetch/                       # HTTP GET → object MinIO (tái sử dụng)
├── src/autoloader/                  # file mới trên MinIO → INSERT staging bảng (tái sử dụng)
├── src/processing/                  # framework incremental processing & soft-delete
├── src/vn_climate_risk_monitor/     # app: config, lakehouse, CLI nguồn, health
│   ├── open_meteo.py                # planner Open-Meteo + fetch CLI
│   └── load.py                      # nối autoloader vào sources/*.yml
│
├── sources/                         # 1 YAML + 1 SQL / nguồn file→bảng staging
├── processing/                      # config Silver/Gold cho archive và forecast
├── transform/                       # dbt: staging → intermediate → marts
│   ├── models/
│   │   ├── staging/                 # view mỏng 1-1 trên source/seed (`stg_*`)
│   │   ├── intermediate/            # dedup, conform, change-aware MERGE (`int_*`)
│   │   └── marts/                   # star schema: dimension, bridge, fact
│   ├── seeds/                       # reference nhỏ, tĩnh, version-control (ward, grid map)
│   ├── macros/
│   └── tests/
│
├── reference/                       # GeoJSON và văn bản nguồn tĩnh
├── orchestration/
│   ├── cron/                        # schedule template; không chứa business logic
│   └── dags/                        # Airflow DAGs: forecast, archive, maintenance
├── serving/                         # API/dashboard chỉ đọc Gold (Bước 8)
├── scripts/                         # bootstrap, run_processing, healthcheck, maintenance
└── tests/
    ├── unit/
    ├── integration/
    └── fixtures/
```

Ingestion Open-Meteo gồm hai lệnh: `fetch-open-meteo` (missing rows + GET/PUT lên
MinIO) và `load-sources` (autoloader nạp vào staging `silver.stg_*`). Nguồn geography
(danh mục phường và ánh xạ ô lưới) đến từ `transform/seeds/*.csv`
(`ward_coordinates_seed.csv` và `ward_grid_map_seed.csv`), không qua collector hay
database nguồn PostgreSQL `public.wards`. Lệnh `make bootstrap-geography` seed và
build trực tiếp graph `+dim_ward` vào DuckLake.

## Bố trí vật lý trên MinIO

```text
s3://vn-climate/
├── bronze/
│   └── files/                       # payload nguyên bản, fetch quản lý
│       └── open_meteo/<dataset>/{forecast, backfill, ifs}/...
├── silver/<ducklake-table>/          # Parquet: staging (stg_*) + intermediate (int_*)
└── gold/<ducklake-table>/            # Parquet: marts (dim_*, bridge_*, fct_*)
```

`bronze/files` do fetch quản lý và immutable. Parquet của `silver` và `gold` do
catalog DuckLake quản lý. Thủ tục `make clean-lake` chỉ dọn snapshot/file mồ côi
trong catalog DuckLake, không bao giờ xóa file trong `bronze/files`.

## Trách nhiệm từng layer

| Layer / Thư mục | Trách nhiệm |
|---|---|
| `bronze/files` | Response nguồn nguyên bản, append-only trên MinIO |
| `silver` (staging) | Bảng append-only do autoloader nạp (`stg_*`) + view mỏng dbt (`stg_*`) |
| `silver` (intermediate) | Dedup theo (ô, giờ), conform, change-aware MERGE (`int_weather_archive_hourly`) |
| `gold` (marts) | Star schema: dimension (`dim_*`), bridge (`bridge_*`), fact (`fct_*`) |
| Control plane | PostgreSQL `ingestion` (file ledger, lease) và `processing` (state, audit runs) |

Thêm nguồn REST mới = planner trong app + `fetch.land`. Thêm nguồn file đã có trên MinIO = 1 cặp
`sources/<tên>.yml` + `<tên>.sql`.

## Quy ước đặt tên

- Schema đã biểu đạt layer nên không dùng `_raw` hoặc `_cleaned`.
- Staging dùng tiền tố `stg_`, ví dụ `stg_weather_archive_hourly` (bảng vật lý do autoloader ghi),
  `stg_open_meteo__weather_archive_hourly` (view dbt), `stg_seed__ward`, `stg_seed__ward_grid`.
- Intermediate dùng tiền tố `int_`, ví dụ `int_weather_archive_hourly` (curated table incremental).
- Marts dùng `dim_`, `bridge_`, `fct_` theo dimensional modeling, ví dụ
  `dim_grid`, `dim_ward`, `bridge_ward_grid`, `fct_rain_archive_hourly`,
  `fct_rain_forecast_hourly`, `fct_rain_forecast_current_hourly` và
  `fct_rain_pressure_alert`.
- Metadata kỹ thuật dùng tên rõ nghĩa: `_source_file`, `_ingested_at`, `_updated_at`, `_row_hash`.

## Incremental contract

1. `fetch` land JSON as-is lên `bronze/files`, không đăng ký gì — crash giữa
   chừng không tạo file mồ côi vì discovery liệt kê storage.
2. `load-sources` liệt kê prefix nguồn, đối chiếu checkpoint theo object key,
   gắn file mới vào một discovery run `SUCCEEDED` ổn định / nguồn
   (`logical_key=discovery`).
3. Engine claim micro-batch kèm lease (gỡ file kẹt `PROCESSING` khi process chết).
4. DuckDB chạy SQL transform của nguồn (`INSERT ... BY NAME` vào staging `silver.stg_*`).
5. Chỉ sau khi commit staging mới cập nhật file ledger thành `COMMITTED`.
6. Crash giữa bước 4 và 5: lease hết hạn, file được claim lại, INSERT lặp —
   staging at-least-once, dedup và MERGE change-aware ở `silver.int_weather_archive_hourly`.
   Payload luôn giữ để replay, không giả định distributed transaction giữa Postgres và MinIO.
