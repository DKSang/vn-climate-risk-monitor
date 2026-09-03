# Storage modeling (Bước 6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gold DuckLake SSOT với `dim_grid` (`weather_model` + `weather_product`), tọa độ canonicalize 6 số ở Silver view, Gold watermark trên control table Postgres, SCD2 phường từ 2025-07-01, registry điểm úng.

**Architecture:** Autoloader chỉ Bronze (`COMMITTED` = Bronze durable). Gold incremental đọc `ingestion.gold_watermarks.last_successful_ingestion_watermark` (chỉ SUCCEEDED). Silver giữ **view**. Climatology khóa `all_available_by_model_v1`.

**Tech Stack:** dbt-duckdb, DuckLake, Postgres control plane, seeds CSV.

## Global Constraints

- Spec: `docs/06-storage-modeling.md`
- Không `flood_probability` / `flood_depth`; không Q3/Q9; không serving mart
- Không create-then-swap `__dbt_tmp` cho table hoặc incremental
- Silver `+materialized: view` — không đổi thành table
- Tọa độ: `ROUND(..., 6)` ở Silver **trước** MD5 và mapping
- `weather_model ∈ {era5, ecmwf_ifs}`; `weather_product ∈ {archive, forecast}`
- Forecast dim: `weather_model=ecmwf_ifs`, `weather_product=forecast`
- Watermark Gold **không** lấy `MAX(_ingested_at)` từ bảng Gold
- SCD2 không reconstruct ranh giới trước 2025
- Không commit trừ khi user yêu cầu

## Files

| File | Role |
|---|---|
| `src/autoloader/schema.py` | DDL `gold_transform_runs` + `gold_watermarks` |
| `src/vn_climate_risk_monitor/` | Đọc/ghi watermark (sau Gold SUCCEEDED) |
| `transform/macros/materializations.sql` | Incremental DuckLake DELETE+INSERT |
| `transform/models/silver/archive_hourly.sql` | lat6/lon6 |
| `transform/models/silver/forecast_hourly.sql` | lat6/lon6, `weather_product=forecast` |
| `transform/models/silver/ward_grid_map.sql` | Map trên lat6/lon6 |
| `transform/models/gold/dim_grid.sql` | Dim ô |
| `transform/seeds/climatology_window_seed.csv` | `all_available_by_model_v1` |
| `transform/models/gold/fct_rainfall_historical_hourly.sql` | Merge theo control watermark |
| `transform/models/gold/fct_rainfall_climatology_monthly.sql` | `climatology_window_id` + years |
| `transform/models/gold/fct_rainfall_historical_anomaly_hourly.sql` | Join cùng window |
| `transform/models/gold/dim_hanoi_ward.sql` | SCD2 từ 2025-07-01 |
| `transform/seeds/hanoi_flood_points_seed.csv` | Điểm úng |
| `tests/unit/test_*gold_watermark*` | Watermark chỉ tăng khi SUCCEEDED |

---

### Task 1: Control table Gold watermark

**Files:**
- Modify: `src/autoloader/schema.py` — thêm DDL
- Modify: `scripts/bootstrap.py` nếu cần `ensure_ingestion_state`
- Create: helper đọc/ghi watermark (cùng Postgres control plane)
- Test: `tests/unit/test_gold_watermark.py`

DDL:

```sql
CREATE TABLE IF NOT EXISTS ingestion.gold_transform_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    source_watermark_from TIMESTAMPTZ,
    last_successful_ingestion_watermark TIMESTAMPTZ,
    started_at_utc TIMESTAMPTZ NOT NULL,
    completed_at_utc TIMESTAMPTZ,
    error_message TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingestion.gold_watermarks (
    pipeline_name TEXT PRIMARY KEY,
    last_successful_ingestion_watermark TIMESTAMPTZ NOT NULL,
    last_succeeded_run_id UUID,
    status TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    updated_at_utc TIMESTAMPTZ NOT NULL
);
```

Hợp đồng test:

- `read_watermark` bỏ qua hàng `FAILED`; không hàng SUCCEEDED → full refresh
- `advance_watermark` chỉ gọi sau khi caller báo Gold commit xong
- Fail Gold → `FAILED`, watermark cũ giữ nguyên

- [ ] **Step 1:** Test fail (chưa có bảng/helper).
- [ ] **Step 2:** DDL + helper.
- [ ] **Step 3:** `uv run pytest tests/unit/test_gold_watermark.py` PASS.

---

### Task 2: Canonical lat/lon ở Silver (view)

**Files:**
- Modify: `transform/models/silver/archive_hourly.sql`
- Modify: `transform/models/silver/forecast_hourly.sql`
- Modify: `transform/models/silver/ward_grid_map.sql`
- Test: `transform/tests/assert_silver_coords_are_canonical.sql`

```sql
ROUND(grid_latitude, 6)  AS grid_latitude,
ROUND(grid_longitude, 6) AS grid_longitude
```

`forecast_hourly`: thêm `weather_model = 'ecmwf_ifs'`, `weather_product = 'forecast'`; `grid_cell_id` từ lat6/lon6 + hai cột đó.

`archive_hourly`: `weather_product = 'archive'`.

Test: mọi lat/lon trên silver weather `ROUND(x,6) = x`; không còn `weather_model = 'forecast'`.

- [ ] **Step 1:** Test fail.
- [ ] **Step 2:** Sửa view (vẫn `materialized='view'`).
- [ ] **Step 3:** `dbt build --select silver.archive_hourly silver.forecast_hourly silver.ward_grid_map` PASS.

---

### Task 3: Macro incremental DuckLake

Giữ như plan cũ: `DELETE`+`INSERT` vào tên đích; không `__dbt_tmp`.

- [ ] **Step 1:** Macro.
- [ ] **Step 2:** `dbt run --select dim_hanoi_ward` regression.

---

### Task 4: `dim_grid` + seed climatology window

**Files:**
- Create: `transform/models/gold/dim_grid.sql`
- Create: `transform/seeds/climatology_window_seed.csv`
- Create: `transform/models/gold/dim_climatology_window.sql` (hoặc seed trực tiếp)
- Test: `transform/tests/assert_dim_grid_covers_archive_and_forecast.sql`

```sql
SELECT
    MD5(CONCAT_WS('|', weather_model, weather_product,
        PRINTF('%.6f', grid_latitude), PRINTF('%.6f', grid_longitude))) AS grid_cell_id,
    weather_model,
    weather_product,
    grid_latitude,
    grid_longitude
FROM (
    SELECT DISTINCT weather_model, 'archive', grid_latitude, grid_longitude
    FROM {{ ref('archive_hourly') }}
    UNION
    SELECT DISTINCT weather_model, 'forecast', grid_latitude, grid_longitude
    FROM {{ ref('forecast_hourly') }}
) s
```

Seed:

```text
climatology_window_id,weather_model,weather_product,definition
all_available_by_model_v1,era5,archive,empirical all available years for this model
all_available_by_model_v1,ecmwf_ifs,archive,empirical all available years for this model
```

`start_year`/`end_year` ghi lúc build climatology fact, không hard-code năm cuối.

- [ ] **Step 1:** Test fail.
- [ ] **Step 2:** dim_grid + seed.
- [ ] **Step 3:** `dbt build --select dim_grid dim_climatology_window` PASS.

---

### Task 5: Fact chỉ `grid_cell_id`

Join `dim_grid`; bỏ lat/lon ô trên fact. Forecast/historical dùng cùng `grid_cell_id`. Orphan test LEFT JOIN dim_grid.

- [ ] Test orphan fail → sửa Gold → `dbt build` PASS.

---

### Task 6: Incremental hourly theo control watermark

**Không** `WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})`.

```python
watermark = read_succeeded_watermark("rainfall_historical_hourly")
# trong dbt: var hoặc pre-hook Python / macro query Postgres
```

Pre-hook: `status=RUNNING`. Post-hook **sau** Gold dependents: `advance_watermark(max_ingested_at of new slice)` chỉ nếu dbt run thành công.

Lookback 72h như spec §7.

Test unit: Gold fail → watermark không tăng. Test SQL fixture rolling 10:00.

Climatology/anomaly/event/drought full table.

- [ ] Fixture lookback.
- [ ] Model + hooks watermark.
- [ ] `--full-refresh` rồi incremental.
- [ ] `dbt build --select +tag:historical` PASS.

---

### Task 7: Climatology + anomaly khóa window

**Files:**
- Modify: `fct_rainfall_climatology_monthly.sql` — thêm `climatology_window_id`, `climatology_start_year`, `climatology_end_year`
- Modify: `fct_rainfall_historical_anomaly_hourly.sql` — join thêm `climatology_window_id`
- Test: không anomaly nếu window lệch; `accepted_values` window id

- [ ] Test fail → cột + join → build PASS.

---

### Task 8: SCD2 từ 2025-07-01

Một version `nq_1656_2025`. Không task GitHub lịch sử. Replay ghi `as_of_boundary_version`.

- [ ] Schema SCD2 + tests unique `(ward_code, valid_from_utc)`.
- [ ] `dbt build --select +dim_hanoi_ward +fct_ward_rainfall_forecast_summary` PASS.

---

### Task 9: Điểm úng

Như plan trước, map trên lat6 + `(weather_model, weather_product)`. Cổng CSV.

Bridge forecast: `weather_product='forecast'`, `weather_model='ecmwf_ifs'`.

---

### Task 10: Docs

Cập nhật `docs/README.md`, `03-architecture.md`, `transform/README.md`, số dòng sau `dbt build`.

---

## Verification

```bash
uv run pytest tests/unit/test_gold_watermark.py
cd transform && uv run dbt build --profiles-dir .
```

Expected: `ERROR=0`. Watermark không tăng nếu dbt fail. Silver vẫn view. `dim_grid` không có `weather_model='forecast'`.
