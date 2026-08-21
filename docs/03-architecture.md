# Kiến trúc hệ thống

**Hanoi Flood & Climate Risk Monitor** · v2.0 · 2026-08-21

## Kiến trúc tổng thể

```text
Sources
  ├── PostgreSQL administrative reference
  ├── versioned CSV/GeoJSON reference
  └── Open-Meteo (chưa triển khai)
            │
            ▼
MinIO + DuckLake
  ├── Bronze: source-faithful, append/replayable
  ├── Silver: validated and conformed
  └── Gold: business-ready dimensions, facts and aggregates
            │
            ▼
dbt quality gate → serving
```

Postgres lưu metadata DuckLake; MinIO lưu source objects và Parquet; DuckDB là
compute engine; dbt quản lý transformation.

## Layer contract

| Layer | Contract | Ví dụ |
|---|---|---|
| Bronze files | Payload nguồn nguyên bản, immutable, có manifest/checksum | `bronze/files/open_meteo/...` |
| Bronze tables | Parse cấu trúc, giữ mọi record/vintage, chưa validate | `bronze_store.tables.open_meteo_forecast_hourly` |
| Silver | Type, validate, dedup, late data, mapping và join | `silver.rainfall_forecast_hourly` |
| Gold | Dimensional model, KPI và aggregate nghiệp vụ | `gold.fct_flood_risk_hourly` |

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

`ops` là schema control-plane, không phải data layer:

```text
ops.pipeline_runs
ops.ingestion_files
```

File discovery dựa trên immutable path và file ledger. Loader chỉ xử lý run có
`_SUCCESS`, ghi staging rồi `MERGE` vào Bronze bằng deterministic row id. Hai
catalog không được giả định có distributed transaction: commit Bronze trước, rồi
đánh dấu file `COMMITTED`; nếu crash ở giữa thì retry cùng deterministic id.

Pattern này học theo các thuộc tính cốt lõi của Databricks Auto Loader:

- incremental file discovery;
- checkpoint riêng cho mỗi pipeline;
- immutable files và không overwrite;
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
- `ops` ingestion ledger;
- geography Bronze/Silver/Gold;
- package boundary cho collectors/loaders/state.

Chưa có:

- collector hoặc loader Open-Meteo;
- các bảng rainfall/flood-risk;
- orchestration và serving.

Chi tiết cây code: [03a-repo-structure.md](03a-repo-structure.md). Contract
ingestion: [04-ingestion.md](04-ingestion.md).

## Tham khảo

- [Databricks medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [DuckLake transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)
- [DuckLake constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints)
