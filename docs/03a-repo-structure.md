# Cấu trúc repository

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-08-21

Repository dùng ba data layer `bronze → silver → gold`. PostgreSQL schema
`ingestion` là control plane cho checkpoint và audit, không phải data layer thứ tư.

## Cây thư mục

```text
vn-climate-risk-monitor/
├── src/autoloader/                  # package GENERIC: nạp file → bảng, đúng một lần mỗi file
│   ├── engine.py                    # discovery → checkpoint → SQL transform → commit
│   ├── checkpoint.py                # PostgreSQL repository: run/file state, claim, lease
│   ├── config.py                    # khai báo nguồn bằng YAML (không phải code)
│   ├── discovery.py                 # directory listing trên object storage
│   ├── http.py                      # session retry + pacer hạn mức effective-call
│   ├── models.py                    # RunAttempt, ClaimedObject
│   ├── schema.py                    # DDL control plane (ingestion.*)
│   └── provero_ducklake.py          # connector Provero đọc qua catalog DuckLake
│
├── src/vn_climate_risk_monitor/     # code riêng của dự án
│   ├── config.py                    # cấu hình typed từ environment
│   ├── lakehouse.py                 # kết nối DuckDB + DuckLake
│   ├── storage/minio.py             # tạo MinIO client, ensure bucket
│   └── ingestion/
│       ├── fetch.py                 # Open-Meteo API → JSON as-is lên MinIO
│       └── run.py                   # nối autoloader vào ingestion/sources/*.yml
│
├── ingestion/sources/               # khai báo nguồn: 1 YAML + 1 SQL cho mỗi nguồn
├── transform/                       # dbt + DuckDB + DuckLake
│   ├── models/
│   │   ├── bronze/                  # source-faithful; không hậu tố `_raw`
│   │   ├── silver/                  # validate, dedup, conform
│   │   └── gold/                    # dimension, fact, KPI nghiệp vụ
│   ├── seeds/                       # reference nhỏ, tĩnh, version-control
│   ├── macros/
│   └── tests/
│
├── reference/                       # GeoJSON và văn bản nguồn tĩnh (chưa tiêu thụ)
├── orchestration/cron/              # schedule template; không chứa business logic
├── serving/                         # API/dashboard chỉ đọc Gold (Bước 8, chưa cài)
├── scripts/                         # bootstrap, maintenance
└── tests/
    ├── unit/
    ├── integration/
    └── fixtures/
```

Ingestion Open-Meteo là hai lệnh: `fetch-open-meteo` (land JSON) và
`load-sources` (autoloader nạp vào Bronze). Nguồn geography (GSO) **không có
collector** — cập nhật rất chậm nên nạp thủ công vào PostgreSQL nguồn, dbt đọc
trực tiếp qua attach `pg_source`.

## Bố trí vật lý trên MinIO

```text
s3://vn-climate/
├── bronze/
│   ├── files/                       # payload nguyên bản, fetch quản lý
│   │   └── open_meteo/<dataset>/{incremental/YYYY/MM/DD/HH, backfill/year=YYYY/month=MM}/
│   └── tables/                      # Parquet do DuckLake quản lý
│       └── <table>/
├── silver/<ducklake-table>/
└── gold/<ducklake-table>/
```

`bronze/files` và `bronze/tables` có owner/lifecycle tách biệt.
Các thủ tục maintenance DuckLake chỉ xóa file đã được catalog quản lý; fetch
không overwrite file nguồn (skip theo từng file đã có).

## Trách nhiệm từng layer

| Layer | Trách nhiệm |
|---|---|
| Bronze files | Response nguồn nguyên bản, append-only |
| Bronze tables | Parse cấu trúc nguồn; được explode array nhưng không lọc/dedup |
| Silver | Schema enforcement, type casting, validation, dedup, mapping, join |
| Gold | Dimension/fact, rolling/forecast KPI, scenario và pressure feature |
| Ingestion control | PostgreSQL run/file state, lease, retry, parser version và lỗi |

Thêm nguồn mới = thêm 1 cặp `ingestion/sources/<tên>.yml` + `<tên>.sql`, không
viết Python (xem runbook 04b). Parser và Bronze table vẫn source-specific để
tránh một generic parser đầy nhánh điều kiện.

## Quy ước đặt tên

- Schema đã biểu đạt layer nên không dùng `_raw` hoặc `_cleaned`.
- Bronze ưu tiên `<source>_<entity>` khi cần tránh trùng tên, ví dụ
  `gso_wards`, `open_meteo_forecast_hourly`.
- Silver dùng tên entity đã chuẩn hóa, ví dụ `wards`, `ward_centroids`,
  `rainfall_forecast_hourly`.
- Gold dùng `dim_`, `fct_` hoặc tên aggregate nghiệp vụ.
- Metadata kỹ thuật dùng tên rõ nghĩa như `source_file_path`, `run_id`,
  `loaded_at_utc`; không dùng tên layer trong tên entity.

## Incremental contract

1. `fetch` land JSON as-is lên `bronze/files`, không đăng ký gì — crash giữa
   chừng không tạo file mồ côi vì discovery liệt kê storage.
2. `load-sources` liệt kê prefix nguồn, đối chiếu checkpoint theo object key,
   đăng ký file mới (run `SUCCEEDED` sau khi đủ file PENDING).
3. Engine claim micro-batch bằng `FOR UPDATE SKIP LOCKED` + lease.
4. DuckDB chạy SQL transform của nguồn (`INSERT ... BY NAME` vào bảng đích).
5. Chỉ sau Bronze commit mới cập nhật file ledger thành `COMMITTED`.
6. Crash giữa bước 4 và 5: lease hết hạn, file được claim lại, INSERT lặp —
   Bronze at-least-once, Silver dedup theo (ô lưới, giờ). Payload luôn giữ để
   replay, không giả định distributed transaction giữa hai catalog.
