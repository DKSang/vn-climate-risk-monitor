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
Fetch (Python) + autoloader: MinIO source objects + PostgreSQL checkpoint
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

Hai bảng giữ checkpoint file và một discovery run ổn định / nguồn
(`logical_key=discovery`). File ledger: `object_key`, status, retry, lease,
error. `file_parameters` JSONB nhỏ (etag/size lúc listing). Không checksum
SHA-256, không mint logical run theo timestamp mỗi lần load.

Fetch land JSON lên MinIO, không đăng ký control plane. Autoloader liệt kê
storage, gắn file mới vào run `SUCCEEDED` của nguồn, claim kèm lease, `INSERT`
Bronze bằng SQL. PostgreSQL và DuckLake không có distributed transaction:
commit Bronze trước, rồi `COMMITTED`; crash giữa chừng thì lease hết hạn, claim
lại, INSERT lặp (at-least-once; Silver dedup).

Pattern học Auto Loader, thu gọn cho single-node:

- incremental discovery qua directory listing + file ledger;
- một checkpoint / nguồn, không collector attempt;
- immutable files, không overwrite;
- lease recovery khi process chết giữa batch (`PROCESSING`);
- `_rescued_data` trong SQL transform cho giá trị không ép kiểu được;
- available-now micro-batch.

Parser và Bronze schema không generic: mỗi source giữ contract/table riêng.
Silver là nơi conform forecast, archive và observation về semantic dùng chung.
Archive source files dùng source-time layout `backfill/year=YYYY/month=MM`;
year plan là planning group, monthly window là logical recovery checkpoint, còn
month × location batch là file checkpoint. Bronze table partition vật lý theo
`year(observed_time_utc)`.

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
- fetch Open-Meteo: missing rows + urllib GET/PUT JSON bất biến trên MinIO;
- autoloader: directory listing, discovery run ổn định, claim/lease, SQL transform;
- forecast hourly production 126 phường/xã;
- archive monthly backfill + pacing theo `OPEN_METEO_MAX_EFFECTIVE_CALLS_PER_HOUR`.

Chưa có:

- các bảng rainfall/scenario/pressure;
- serving.

Chi tiết cây code: [03a-repo-structure.md](03a-repo-structure.md). Ingestion hiện hành:
[04b-ingestion-runbook.md](04b-ingestion-runbook.md). Ghi chép thiết kế/khảo sát API cũ:
[04-ingestion.md](04-ingestion.md). Phương pháp KPI:
[05-kpi-methodology.md](05-kpi-methodology.md).

## Tham khảo

- [Databricks medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [DuckLake transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)
- [DuckLake constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints)
