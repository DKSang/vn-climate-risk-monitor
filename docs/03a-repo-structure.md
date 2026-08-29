# Cấu trúc repository

**Hanoi Flood & Climate Risk Monitor** · v1.0 · 2026-08-21

Repository dùng ba data layer `bronze → silver → gold`. PostgreSQL schema
`ingestion` là control plane cho checkpoint và audit, không phải data layer thứ tư.

## Cây thư mục

```text
vn-climate-risk-monitor/
├── src/fetch/                       # HTTP GET → object MinIO (tái sử dụng)
├── src/autoloader/                  # file mới trên MinIO → INSERT bảng (tái sử dụng)
├── src/vn_climate_risk_monitor/     # app: config, lakehouse, CLI nguồn
│   ├── open_meteo.py                # planner Open-Meteo + fetch CLI
│   └── load.py                      # nối autoloader vào sources/*.yml
│
├── sources/                         # 1 YAML + 1 SQL / nguồn file→bảng
├── transform/                       # dbt bronze → silver → gold
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

Ingestion Open-Meteo là hai lệnh: `fetch-open-meteo` (missing rows + GET/PUT) và
`load-sources` (autoloader nạp vào Bronze). Nguồn geography (GSO) **không có
collector** — cập nhật rất chậm nên nạp thủ công vào PostgreSQL nguồn, dbt đọc
trực tiếp qua attach `pg_source`.

## Bố trí vật lý trên MinIO

```text
s3://vn-climate/
├── bronze/
│   ├── files/                       # payload nguyên bản, fetch quản lý
│   │   └── open_meteo/<dataset>/{incremental/YYYY/MM/DD/HH,
│   │                             backfill/year=YYYY/month=MM,   # era5
│   │                             ifs/year=YYYY/month=MM}/        # ecmwf_ifs
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

Thêm nguồn REST mới = planner trong app + ``fetch.land``. Thêm nguồn file đã có trên MinIO = 1 cặp
`sources/<tên>.yml` + `<tên>.sql`.

## Quy ước đặt tên

- Schema đã biểu đạt layer nên không dùng `_raw` hoặc `_cleaned`.
- Bronze ưu tiên `<source>_<entity>` khi cần tránh trùng tên, ví dụ
  `gso_wards`, `open_meteo_forecast`, `open_meteo_archive`, `open_meteo_ifs`.
- Silver dùng tên entity đã chuẩn hóa, ví dụ `wards`, `ward_centroids`,
  `rainfall_forecast_hourly`.
- Gold dùng `dim_`, `fct_` hoặc tên aggregate nghiệp vụ.
- Metadata kỹ thuật dùng tên rõ nghĩa như `source_file_path`, `run_id`,
  `loaded_at_utc`; không dùng tên layer trong tên entity.

## Incremental contract

1. `fetch` land JSON as-is lên `bronze/files`, không đăng ký gì — crash giữa
   chừng không tạo file mồ côi vì discovery liệt kê storage.
2. `load-sources` liệt kê prefix nguồn, đối chiếu checkpoint theo object key,
   gắn file mới vào một discovery run `SUCCEEDED` ổn định / nguồn
   (`logical_key=discovery`).
3. Engine claim micro-batch kèm lease (gỡ file kẹt `PROCESSING` khi process chết).
4. DuckDB chạy SQL transform của nguồn (`INSERT ... BY NAME` vào bảng đích).
5. Chỉ sau Bronze commit mới cập nhật file ledger thành `COMMITTED`.
6. Crash giữa bước 4 và 5: lease hết hạn, file được claim lại, INSERT lặp —
   Bronze at-least-once, Silver dedup theo (ô lưới, giờ). Payload luôn giữ để
   replay, không giả định distributed transaction giữa hai catalog.
