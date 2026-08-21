# Cấu trúc repository

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-08-21

Repository dùng ba data layer `bronze → silver → gold`. PostgreSQL schema
`ingestion` là control plane cho checkpoint và audit, không phải data layer thứ tư.

## Cây thư mục

```text
vn-climate-risk-monitor/
├── src/vn_climate_risk_monitor/
│   ├── config.py                    # cấu hình typed từ environment
│   ├── lakehouse.py                 # kết nối DuckDB + DuckLake
│   ├── storage/
│   │   └── minio.py                 # adapter object storage dùng chung
│   └── ingestion/
│       ├── layout.py                # contract object key bronze/files
│       ├── collectors/              # API → response JSON bất biến trên MinIO
│       ├── loaders/                 # bronze/files → Bronze DuckLake table
│       ├── pipelines/               # compose collect → drain loader
│       ├── observability.py         # health/metrics từ control plane
│       ├── scheduling.py            # deterministic hourly logical slot
│       └── state/                   # PostgreSQL schema/repository/checkpoint
│
├── transform/                       # dbt + DuckDB + DuckLake
│   ├── models/
│   │   ├── bronze/                  # source-faithful; không hậu tố `_raw`
│   │   ├── silver/                  # validate, dedup, conform
│   │   └── gold/                    # dimension, fact, KPI nghiệp vụ
│   ├── seeds/                       # reference nhỏ, tĩnh, version-control
│   ├── macros/
│   └── tests/
│
├── reference/                       # GeoJSON và văn bản nguồn tĩnh
├── orchestration/cron/              # schedule template; không chứa business logic
├── serving/                         # API/dashboard chỉ đọc Gold
├── scripts/                         # bootstrap, verify, maintenance
└── tests/
    ├── unit/
    ├── integration/
    └── fixtures/
```

Không còn package `ingest/` ở repository root. Ingestion là code ứng dụng và nằm
trong package cài đặt được `vn_climate_risk_monitor.ingestion`.

## Bố trí vật lý trên MinIO

```text
s3://vn-climate/
├── bronze/
│   ├── files/                       # payload nguyên bản, collector quản lý
│   │   └── <source>/<dataset>/<load_type>/YYYY/MM/DD/HH/<run_id>/
│   └── tables/                      # Parquet do DuckLake quản lý
│       └── <table>/
├── silver/<ducklake-table>/
└── gold/<ducklake-table>/
```

`bronze/files` và `bronze/tables` có owner/lifecycle tách biệt.
Các thủ tục maintenance DuckLake chỉ xóa file đã được catalog quản lý; collector
không overwrite file nguồn.

## Trách nhiệm từng layer

| Layer | Trách nhiệm |
|---|---|
| Bronze files | Response nguồn nguyên bản, append-only |
| Bronze tables | Parse cấu trúc nguồn; được explode array nhưng không lọc/dedup |
| Silver | Schema enforcement, type casting, validation, dedup, mapping, join |
| Gold | Dimension/fact, rolling/forecast KPI, scenario và pressure feature |
| Ingestion control | PostgreSQL run/file state, checksum, lease, parser version và lỗi |

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

1. Collector tạo attempt `RUNNING` và file `PENDING` trong PostgreSQL.
2. Collector chỉ ghi immutable response JSON vào `bronze/files`.
3. Khi đủ batch hợp lệ, attempt chuyển `SUCCEEDED`.
4. Loader claim file bằng transaction, `SKIP LOCKED` và lease.
5. Parser ghi staging, sau đó `MERGE` theo deterministic row id và commit Bronze.
6. Chỉ sau Bronze commit mới cập nhật file ledger thành `COMMITTED`.
7. Crash giữa hai commit sẽ retry; deterministic id ngăn duplicate. Payload luôn
   được giữ để replay, không dựa vào distributed transaction giữa hai catalog.

Open-Meteo forecast collector, PostgreSQL discovery/checkpoint và Bronze hourly
loader đã được triển khai. Collector, parser, object reader và loader vẫn tách
module để HTTP, schema parsing, storage và checkpoint có thể test độc lập.
Phase 5 chỉ compose các module này; cron không chứa parsing hoặc state logic.
