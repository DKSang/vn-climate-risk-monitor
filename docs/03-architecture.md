# Kiến trúc hệ thống

**Hanoi Flood & Climate Risk Monitor** · v2.2 · 2026-09-08

## Kiến trúc tổng thể

```text
Sources
  ├── versioned CSV reference (seeds)
  └── Open-Meteo REST
            │
            ▼
Fetch (Python): MinIO source landing zone (`bronze/files/...`)
            │
            ▼
Autoloader: file ledger (`ingestion`) + INSERT staging (`catalog1.silver.stg_*`)
            │
            ▼
DuckLake (catalog1)
  ├── Silver: staging (`stg_*`) + intermediate curated (`int_weather_archive_hourly`, change-aware MERGE)
  └── Gold: marts (`dim_*`, `bridge_*`, `fct_*`), gồm forecast history/current
      và `fct_rain_pressure_alert`
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
- Airflow và dashboard chạy từ image build theo `uv.lock`; không bind-mount
  source production hoặc cài dependency lại khi khởi động;
- image nền được pin cả version lẫn digest; container ứng dụng chạy non-root;
- Compose đưa password/key qua `/run/secrets`, application đọc biến `*_FILE`;
  secret không nằm trong `docker inspect` environment;
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
| Bronze files | Response nguồn nguyên bản, immutable; ETag/size ở file ledger | `bronze/files/open_meteo/...` |
| Silver staging | Parse cấu trúc, giữ mọi record/vintage, chưa validate | `silver.stg_weather_forecast` |
| Silver curated | Type, validate, dedup, late data, mapping và join | `silver.int_weather_archive_hourly` |
| Gold | Dimensional model, KPI và aggregate nghiệp vụ | `gold.fct_rain_archive_hourly` |

Silver staging có thể explode array nguồn thành grain nguyên tử vì payload
nguyên bản đã được giữ trong `bronze/files`. Không được lọc, dedup hay áp
business rule khi land raw object; các quy tắc đó thuộc Silver curated.

## Naming

Schema đã thể hiện layer, do đó không dùng hậu tố `_raw` hoặc `_cleaned`.
Cấu trúc dbt theo chuẩn 3 lớp (`staging / intermediate / marts`); tên vật lý
DuckLake vẫn theo medallion:

```text
silver.stg_weather_archive_hourly      staging append-only (autoloader ghi, NGOÀI dbt)
silver.stg_weather_forecast

silver.stg_open_meteo__weather_archive_hourly   staging dbt: view mỏng trên source
silver.stg_seed__ward                           staging dbt: view mỏng trên seed
silver.stg_seed__ward_grid

silver.int_weather_archive_hourly      curated: dedup + MERGE change-aware (incremental)

gold.dim_grid
gold.dim_ward
gold.bridge_ward_grid
gold.fct_rain_archive_hourly
gold.fct_rain_forecast_hourly              forecast history theo vintage
gold.fct_rain_forecast_current_hourly      serving view của run mới nhất
gold.fct_rain_pressure_alert               tín hiệu áp lực mưa vận hành
```

Tiền tố `stg_` đánh dấu lớp staging append-only (vật lý) hoặc view mỏng
(dbt); `int_` là lớp trung gian mang business logic (dedup, conform); Gold
dùng `dim_`/`fct_`/`bridge_` theo dimensional modeling.

## Incremental ingestion

`ingestion` và `processing` là native PostgreSQL control-plane schema, không phải
data layer. Hai schema TÁCH BIỆT vì trả lời hai câu hỏi khác nhau:

```text
ingestion.ingestion_runs      ┐  File này đã commit vào Silver staging chưa?
ingestion.ingestion_files     ┘  (file checkpoint)

processing.processing_state   ┐  Process này đã xử lý upstream tới mốc nào?
processing.processing_runs    ┘  (checkpoint + Gold published snapshot)
```

Trộn chúng là cách chắc chắn nhất để một trong hai câu trả lời sai: cùng một
upstream có thể nuôi nhiều process với tiến độ khác nhau, nên checkpoint xử
lý thuộc về PROCESS chứ không thuộc về source.

Processing checkpoint hiện hành được mô tả trong
[06-storage-modeling](06-storage-modeling.md#4-processing-checkpoint).

Hai bảng `ingestion` giữ checkpoint file và một discovery run ổn định / nguồn
(`logical_key=discovery`). File ledger: `object_key`, status, retry, lease,
error. `file_parameters` JSONB nhỏ (etag/size lúc listing). Không checksum
SHA-256, không mint logical run theo timestamp mỗi lần load.

Fetch land JSON lên MinIO, không đăng ký control plane. Autoloader liệt kê
storage, gắn file mới vào run `SUCCEEDED` của nguồn, claim kèm lease, `INSERT`
Silver staging bằng SQL. PostgreSQL và DuckLake không có distributed
transaction: commit staging trước, rồi đánh file `COMMITTED`; crash giữa chừng thì
lease hết hạn, claim lại, INSERT lặp (at-least-once; Silver curated dedup).

Pattern học Auto Loader, thu gọn cho single-node:

- incremental discovery qua directory listing + file ledger;
- một checkpoint / nguồn, không collector attempt;
- immutable files, không overwrite;
- lease recovery khi process chết giữa batch (`PROCESSING`);
- `_rescued_data` trong SQL transform cho giá trị không ép kiểu được;
- available-now micro-batch.

Parser và Silver staging schema không generic: mỗi source giữ contract/table riêng.
Silver là nơi conform forecast, archive và observation về semantic dùng chung.
Archive source files dùng source-time layout `backfill/year=YYYY/month=MM`;
year plan là planning group, monthly window là logical recovery checkpoint, còn
month × location batch là file checkpoint. Silver staging table partition vật lý theo
`year(observed_time_utc)`.

## Materialization

| Lớp dbt | Mặc định hiện tại | Lý do |
|---|---|---|
| `staging` | DuckLake **view** | Không tốn dung lượng — source vật lý đã nằm ở DuckLake, staging chỉ `SELECT *` mỏng |
| `intermediate` | View (mặc định); curated là **incremental table** | MERGE change-aware, watermark `_updated_at`; đặt lên view sẽ tái tạo bug 49s |
| `marts` | DuckLake **table** | Stable serving contract; dim/bridge `incremental` giữ cờ soft delete |

Custom dbt `table` materialization dùng `CREATE OR REPLACE TABLE` trực tiếp vào
tên đích và giữ đầy đủ hooks/commit. Điều này tránh file DuckLake bị ghi vào
prefix `__dbt_tmp` rồi chỉ rename metadata.

## Data quality

- Silver là nơi schema enforcement, dedup và validation.
- `dbt build` là quality gate trước Gold.
- Test `assert_gold_is_readable` buộc đọc cột VARCHAR từ Parquet, tránh green giả
  khi chỉ `COUNT(*)` từ catalog metadata.
- JSON trong `bronze/files` và Silver staging history cho phép replay khi
  parser/schema thay đổi.

## Trạng thái triển khai

Đã có:

- MinIO + Postgres + DuckLake + DuckDB/dbt;
- một catalog DuckLake (`catalog1`) với hai schema `silver` và `gold`, Bronze là landing zone raw file trên MinIO (`bronze/files/...`);
- native PostgreSQL control plane: schema `ingestion` (file ledger) và `processing` (state / checkpoints);
- geography seed tĩnh (`ward_coordinates_seed`, `ward_grid_map_seed`) + dbt build `dim_ward` / `bridge_ward_grid`;
- fetch Open-Meteo: missing rows + urllib GET/PUT JSON bất biến trên MinIO;
- autoloader: directory listing, discovery run ổn định, claim/lease, SQL transform;
- forecast hourly production 126 phường/xã;
- archive monthly backfill, fetch theo ô lưới (ERA5 trước 2017, ECMWF IFS từ
  2017), song song qua `fetch.pool` (`OPEN_METEO_FETCH_WORKERS`).

Chưa có:

- mô hình xác suất/độ sâu ngập đã hiệu chỉnh;

Chi tiết cây code: [03a-repo-structure.md](03a-repo-structure.md). Ingestion hiện hành:
[04b-ingestion-runbook.md](04b-ingestion-runbook.md). Phương pháp KPI:
[05-kpi-methodology.md](05-kpi-methodology.md).

## Tham khảo

- [Databricks medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [DuckLake transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)
- [DuckLake constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints)
