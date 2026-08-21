# Kiến trúc hệ thống

**Hanoi Flood & Climate Risk Monitor** · v2.0 · 2026-08-21

## Kiến trúc tổng thể

```text
Sources
  ├── PostgreSQL administrative reference
  ├── versioned CSV/GeoJSON reference
  └── Open-Meteo
            │
            ▼
Collector: PostgreSQL control plane + MinIO source objects
            │
            ▼
DuckLake
  ├── Bronze: source-faithful, append/replayable
  ├── Silver: validated and conformed
  └── Gold: business-ready dimensions, facts and aggregates
            │
            ▼
dbt quality gate → serving
```

PostgreSQL lưu cả ingestion control state và metadata DuckLake trong các schema
tách biệt. MinIO lưu source objects và Parquet; DuckDB là compute engine; dbt
quản lý transformation.

## Hồ sơ vận hành

MVP là **portfolio production-like, zero-cost**:

- chạy single-node bằng Docker Compose trên máy sở hữu sẵn hoặc VM free-tier;
- toàn bộ runtime là phần mềm open-source; không phụ thuộc managed service trả phí;
- pipeline phải có schedule, restart/retry, checkpoint, idempotency, quality gate,
  health check, metrics và runbook recovery;
- persistent volume và backup metadata phải tách khỏi vòng đời container;
- không triển khai Kubernetes, multi-region, active-active hoặc high availability;
- nguồn Free API không có uptime guarantee, vì vậy hệ thống chỉ cam kết best-effort
  và phải biểu diễn `DEGRADED`/stale data rõ ràng.

Chi phí mục tiêu 0 đồng/tháng không có nghĩa là tài nguyên vô hạn. Storage,
request budget, CPU/RAM và retention phải có guardrail; khi chuyển sang mục đích
thương mại phải review lại giấy phép và deployment profile.

## Layer contract

| Layer | Contract | Ví dụ |
|---|---|---|
| Bronze files | Response nguồn nguyên bản, immutable; checksum ở PostgreSQL | `bronze/files/open_meteo/...` |
| Bronze tables | Parse cấu trúc, giữ mọi record/vintage, chưa validate | `bronze_store.tables.open_meteo_forecast_hourly` |
| Silver | Type, validate, dedup, late data, mapping và join | `silver.rainfall_forecast_hourly` |
| Gold | Dimensional model, KPI và aggregate nghiệp vụ | `gold.fct_rainfall_pressure_hourly` |

Bronze có thể explode array nguồn thành grain nguyên tử vì payload nguyên bản đã
được giữ trong `bronze/files`. Không được lọc, dedup hay áp business rule tại
Bronze.

## Naming

Schema đã thể hiện layer, do đó không dùng hậu tố `_raw` hoặc `_cleaned`:

```text
bronze_store.tables.gso_provinces
bronze_store.tables.gso_wards
bronze_store.tables.gso_administrative_units
bronze_store.tables.gso_administrative_regions
bronze_store.tables.ward_coordinates

silver.wards
silver.ward_centroids
silver.ward_locations

gold.dim_hanoi_ward
```

Source qualifier ở Bronze giúp tránh xung đột tên model dbt và ghi rõ lineage.
Gold tiếp tục dùng `dim_`/`fct_` theo dimensional modeling.

## Incremental ingestion

`ingestion` là native PostgreSQL control-plane schema, không phải data layer:

```text
ingestion.ingestion_runs
ingestion.ingestion_files
```

Collector đăng ký từng response object vào PostgreSQL. Loader chỉ claim file
`PENDING` thuộc attempt `SUCCEEDED`, xác minh checksum rồi `MERGE` vào Bronze
bằng deterministic row id. PostgreSQL control plane và DuckLake catalog không
được giả định có distributed transaction: commit Bronze trước, rồi đánh dấu file
`COMMITTED`; nếu crash ở giữa thì retry cùng deterministic id.

Pattern này học theo các thuộc tính cốt lõi của Databricks Auto Loader:

- incremental discovery qua PostgreSQL file ledger;
- checkpoint riêng cho mỗi pipeline;
- immutable files và không overwrite;
- claim bằng `FOR UPDATE SKIP LOCKED` và lease recovery;
- rescued data cho schema drift;
- available-now micro-batch cho workload không cần streaming 24/7.

## Materialization

| Layer | Mặc định hiện tại | Lý do |
|---|---|---|
| Bronze | DuckLake table | Persist source-faithful records và lineage |
| Silver | View | Transform địa lý hiện nhẹ và không cần copy dữ liệu |
| Gold | DuckLake table | Stable serving contract và snapshot |

Custom dbt `table` materialization dùng `CREATE OR REPLACE TABLE` trực tiếp vào
tên đích và giữ đầy đủ hooks/commit. Điều này tránh file DuckLake bị ghi vào
prefix `__dbt_tmp` rồi chỉ rename metadata.

## Data quality

- Silver là nơi schema enforcement, dedup và validation.
- `dbt build` là quality gate trước Gold.
- Test `assert_gold_is_readable` buộc đọc cột VARCHAR từ Parquet, tránh green giả
  khi chỉ `COUNT(*)` từ catalog metadata.
- JSON trong `bronze/files` và Bronze history cho phép replay khi parser/schema thay đổi.

## Trạng thái triển khai

Đã có:

- MinIO + Postgres + DuckLake + DuckDB/dbt;
- ba schema medallion;
- native PostgreSQL ingestion control plane;
- geography Bronze/Silver/Gold;
- package boundary cho collectors/loaders/state;
- Open-Meteo forecast collector chỉ ghi immutable response JSON;
- Phase 3 run/file state, logical attempt identity, claim/lease và checksum reader;
- Phase 4 explicit Arrow parser và idempotent Bronze DuckLake loader.
- Phase 5 deterministic schedule slot, collect→load orchestration, quota guardrail,
  timeout/lease recovery và PostgreSQL health metrics.

Chưa có:

- các bảng rainfall/scenario/pressure;
- serving.

Chi tiết cây code: [03a-repo-structure.md](03a-repo-structure.md). Contract
ingestion: [04-ingestion.md](04-ingestion.md). Phương pháp KPI:
[05-kpi-methodology.md](05-kpi-methodology.md).

## Tham khảo

- [Databricks medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [DuckLake transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)
- [DuckLake constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints)
