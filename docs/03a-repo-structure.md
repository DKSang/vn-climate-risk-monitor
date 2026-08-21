# Cấu trúc repository

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-08-21

Repository dùng ba data layer `bronze → silver → gold`. `ops` là schema kỹ thuật
cho checkpoint và audit, không phải data layer thứ tư.

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
│       ├── collectors/              # API/file → JSON/file bất biến trên MinIO
│       ├── loaders/                 # bronze/files → Bronze DuckLake table
│       └── state/                   # ops.pipeline_runs, ops.ingestion_files
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
├── orchestration/                   # DAG chỉ điều phối, không chứa business logic
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
| Bronze files | Request/response/manifest/checksum nguyên bản, append-only |
| Bronze tables | Parse cấu trúc nguồn; được explode array nhưng không lọc/dedup |
| Silver | Schema enforcement, type casting, validation, dedup, mapping, join |
| Gold | Dimension/fact, rolling KPI, risk score, aggregate phục vụ sản phẩm |
| Ops | File ledger, pipeline run, checkpoint, parser version và lỗi |

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

1. Collector tạo một run bất biến trong `bronze/files`.
2. `_SUCCESS` được ghi cuối cùng; loader bỏ qua run thiếu marker này.
3. Loader discover file theo path và đối chiếu `ops.ingestion_files`.
4. Parser ghi staging, sau đó `MERGE` theo deterministic row id và commit Bronze.
5. Chỉ sau Bronze commit mới cập nhật file ledger thành `COMMITTED`.
6. Crash giữa hai commit sẽ retry; deterministic id ngăn duplicate. Payload luôn
   được giữ để replay, không dựa vào distributed transaction giữa hai catalog.

Open-Meteo chưa được triển khai ở phiên bản cấu trúc này. Các package
`collectors/` và `loaders/` mới chỉ định nghĩa ranh giới để bước tiếp theo không
trộn HTTP, object storage, parsing và checkpoint vào cùng một module.
