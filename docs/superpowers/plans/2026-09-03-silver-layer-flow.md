# Silver Layer Flow — áp reference architecture lên stack hiện tại

## Context

Vừa dựng xong medallion gọn (9 model, build 1m01s, PASS=84) nhưng chưa áp pattern
*Silver Layer Flow*: `silver_stg` (append, full history) → `silver_clean`
(dedup + upsert, current active) → soft delete. Đo dữ liệu thật cho thấy thiết
kế hiện tại có một lỗ hổng scale mà pattern đó chữa đúng chỗ.

| Đo được | Con số |
|---|---|
| Bảng staging hiện tại | 19.895.304 dòng |
| Grain thật `(model, ô, giờ)` | 5.818.584 dòng |
| Nhiều phiên bản trên cùng grain | 14.076.720 dòng (70,75%) — tối đa 5 lần/grain |
| Grain có **giá trị mâu thuẫn** | **0** |
| Scan `silver.weather_hourly` (view) | 8,2s |
| Filter incremental **qua view** | **9,7s** |
| Filter incremental **qua table** | **0,0s** |

Hai hệ quả:

**1. Silver phải là table, không phải view.** `incremental_input_scope` /
`incremental_output_scope` gọi subquery lọc **5 lần** mỗi lần chạy. Silver là
view có `QUALIFY`, nên DuckDB không đẩy filter xuống dưới window function →
**~49 giây mỗi lần chạy chỉ để TÌM cửa sổ cần tính**, trước khi tính một dòng
nào. Chi phí tăng tuyến tính theo staging, vĩnh viễn.

**2. MERGE phải change-aware, không chỉ key-aware.** Trong ảnh, customer 3 và 4
không đổi nên `_updated_at` giữ nguyên `2024-12-01`; chỉ dòng thật sự đổi mới
được bump. Với ta điều này là sống còn: 70% bản trùng nhưng **0 mâu thuẫn giá
trị**, nên MERGE ngây thơ sẽ bump `_updated_at` của cả 5,8M dòng → Gold
reprocess toàn bộ → chuỗi incremental sụp ngay ở lần chạy thứ hai.

Ba quyết định đã chốt với người dùng:
- **Xoá catalog `bronze_store`.** Bronze chỉ còn là landing zone raw files; bảng
  `bronze_store.tables.open_meteo_hourly` trở thành **staging của Silver**.
- **`is_active` đồng nhất** trên mọi bảng clean, kể cả weather.
- **Soft delete chỉ cho `dim_ward`/`bridge`**, nhưng tách thành **module tái sử
  dụng cho dự án khác**. Registry `metadata.etl_job` — hoãn.

## Kiến trúc chốt

```
  bronze/files/**.json                    landing zone — raw bất biến, KHÔNG catalog
        │  autoloader · file ledger = ingestion.ingestion_files
        │  APPEND ONLY · source-faithful · giữ MỌI phiên bản đã fetch
        ▼
  catalog1.silver.stg_weather_hourly      ← thay chỗ bronze_store.tables.*
        │  process `silver_weather` · watermark trên _ingested_at
        │  dedup ROW_NUMBER → so _row_hash → MERGE (chỉ dòng ĐỔI THẬT)
        ▼
  catalog1.silver.weather_hourly          ← current active · _updated_at · is_active
        │  process `rain_gold` · watermark trên _updated_at
        ▼
  catalog1.gold.*                         dim / bridge / fact
        │  soft-delete adapter · anti-join seed
        ▼
  dim_ward.is_active · bridge_ward_grid.is_active
```

MỘT catalog DuckLake (`catalog1`). `bronze/` trên MinIO chỉ còn `files/`.

### Vòng đời một dòng (theo ảnh 2)

```
raw fetch #1  (grid G, giờ H, mưa 2.0)  →  stg: 1 dòng, _ingested_at=T1
raw fetch #2  (cửa sổ chồng, mưa 2.0)   →  stg: 2 dòng  ← full history, đúng thiết kế
                                            clean: _row_hash TRÙNG → KHÔNG đụng
                                                   _updated_at vẫn T1
raw fetch #3  (ERA5T→ERA5, mưa 2.4)     →  stg: 3 dòng
                                            clean: _row_hash KHÁC → MERGE
                                                   _updated_at = T3  → Gold nhặt lên
```

70% "trùng" ở staging **không phải rác** — đó là change log, đúng vai
`silver_stg`. Lãng phí nằm ở tầng *fetch* (cửa sổ backfill chồng nhau), không ở
thiết kế lưu trữ.

## Ánh xạ reference architecture → stack của ta

| Thành phần trong ảnh | Quyết định | Lý do |
|---|---|---|
| `bronze_customer` (Delta table) | **Gộp** vào autoloader | File ledger `ingestion_files` đã làm đúng vai "raw incremental + watermark". Thêm bảng nữa là bản sao thứ ba của cùng dữ liệu |
| `silver_stg` (append only) | **Nhận** — chính là bảng autoloader đang ghi, đổi tên | Đã append-only + full history + `_ingested_at` |
| `silver_clean` (dedup + upsert) | **Nhận — thay đổi lớn nhất** | Chữa đúng 49s/lần đo được |
| `ROW_NUMBER() PARTITION BY key ORDER BY _ingested_at DESC` | **Nhận nguyên** | Đã có trong `weather_hourly.sql`, chỉ đổi nơi chạy |
| `_updated_at = now()` khi MERGE | **Nhận + siết** | Ảnh ngụ ý chỉ bump dòng đổi; ta ép bằng `_row_hash` (xem Phase 2) |
| `is_active` mọi dòng clean | **Nhận đồng nhất** | Consumer viết `WHERE is_active` không cần nhớ bảng nào hỗ trợ |
| Watermark/asset (`silver.etl_watermarks`) | **Đã có, đúng hơn** — `processing.processing_state` | Ảnh dùng `_ingested_at > watermark`; ta dùng *start time lần chạy thành công* − `safety_lag`. Xem `docs/superpowers/plans/2026-09-03-processing-checkpoint.md` |
| `metadata.etl_job` (registry) | **Hoãn** | YAML `processing/*.yml` đã khai báo; hai bản sẽ lệch nhau |
| Soft delete qua JDBC anti-join | **Nhận có chọn lọc** — `dim_ward` + `bridge_ward_grid` | Open-Meteo REST không liệt kê được key; danh mục phường thì được, và CÓ bị giải thể |
| Dagster | **Từ chối** — giữ Make + cron | 2 process thì Dagster là hạ tầng nhiều hơn nghiệp vụ |
| Spark / Delta / Polars | Không áp dụng — DuckDB + DuckLake | Đã có materialization riêng ở `transform/macros/materializations.sql` |

---

## Phase 1 — Gộp về một catalog

Bỏ `bronze_store` / `ducklake_bronze`. Autoloader ghi `catalog1.silver.stg_*`.
Đường vật lý đổi `bronze/tables/<table>/` → `silver/stg_<table>/`.

**Sửa (bắt đầu từ file gốc):**
- `src/vn_climate_risk_monitor/lakehouse.py` — bỏ `BRONZE_CATALOG`,
  `BRONZE_METADATA_SCHEMA`, `BRONZE_TABLE_SCHEMA`, tham số `attach_bronze`, ATTACH thứ hai
- `scripts/bootstrap.py`, `scripts/clean_lake.py`, `scripts/reset_lakehouse.py` — một catalog.
  `reset_lakehouse.py` thêm `--keep-staging` để reset Gold mà không phải nạp lại 20M dòng
- `sources/*.yml` (3) — `target: catalog1.silver.stg_weather_hourly` / `stg_weather_forecast`
- `transform/models/sources.yml`, `transform/profiles.yml` (bỏ attach 2),
  `transform/dbt_project.yml` (bỏ 2 hook `ducklake_*('bronze_store')`)
- `src/autoloader/provero_ducklake.py`, `quality/provero.yaml`, `Makefile`
  (`_PROVERO_FORECAST`), `.env.example`, `.github/workflows/ci.yml`
- `src/vn_climate_risk_monitor/health.py` (4 chỗ), `open_meteo.py` (2 chỗ),
  `tests/unit/test_autoloader_config.py`
- Docs: `docs/03-architecture.md`, `04a`, `04b`, `04c`, `04-ingestion.md`, `06`

**Không đổi:** prefix `bronze/files/` — đổi tên phải di dời 790MB và viết lại
`object_key` trong ledger, đổi lấy đúng một cái tên đẹp hơn.

## Phase 2 — Silver clean: MERGE change-aware

`transform/models/silver/weather_hourly.sql` từ `view` → `incremental`,
`unique_key = 'weather_hourly_key'`. Giữ nguyên dedup `QUALIFY ROW_NUMBER()` đã
có — nó đúng, chỉ đổi nơi chạy.

Cột kỹ thuật của lớp mutable:

| Cột | Nghĩa |
|---|---|
| `weather_hourly_key` | `MD5(grid_cell_id \| valid_time_utc)` — merge key |
| `_row_hash` | MD5 của các cột GIÁ TRỊ — dùng để phát hiện đổi thật |
| `_ingested_at` | giữ từ staging — provenance, raw đến lúc nào |
| `_updated_at` | **chỉ bump khi `_row_hash` đổi** — Gold watermark trên cột này |
| `is_active` | luôn `TRUE` cho weather; có để đồng nhất với các bảng clean khác |

Hình dạng model:

```sql
incoming AS (  -- dedup trong lô, theo incremental_input_scope trên staging
    ... QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY _ingested_at DESC) = 1
),
changed AS (
    SELECT i.* FROM incoming i
    LEFT JOIN {{ this }} t USING (weather_hourly_key)
    WHERE t.weather_hourly_key IS NULL      -- dòng mới
       OR t._row_hash <> i._row_hash        -- đổi THẬT
)
```

Materialization `incremental` hiện tại (DELETE keys in slice + INSERT slice) làm
đúng việc: chỉ key có trong slice bị đụng, nên dòng không đổi giữ nguyên
`_updated_at`. **Không cần sửa `materializations.sql`.**

`_updated_at` lấy từ **`run_started_at` của process**, không phải
`CURRENT_TIMESTAMP` của DuckDB — cùng kỷ luật một-đồng-hồ đã áp cho
`_ingested_at`, và start time sớm hơn lúc ghi thật nên an toàn cho watermark
downstream. Runner bơm xuống thành var `processing_run_started_at`
(`src/processing/dbt.py::build_vars`).

**Hai process, hai checkpoint** — `processing_state` đã khoá theo
`(process_key, source_ref, scope)` nên **không cần đổi schema**:

| Process | Source (change column) | Target | dbt select |
|---|---|---|---|
| `silver_weather` | `stg_weather_hourly` (`_ingested_at`) | `silver.weather_hourly` | `tag:silver` |
| `rain_gold` | `weather_hourly` (`_updated_at`) | `gold.fct_rain_hourly` | `tag:gold` |

Tách hai để lỗi ở Gold không buộc Silver dedup lại từ đầu.

**Sửa:** `processing/silver_weather.yml` (mới), `processing/rain_gold.yml` (đổi
tên từ `rain_hourly.yml`), `Makefile` target `transform` chạy tuần tự,
`fct_rain_hourly.sql` đổi `source_ref`/`change_column`. `is_active` + `_updated_at`
thêm cho `silver.ward`, `silver.ward_grid`, `gold.dim_grid`.

## Phase 3 — Soft-delete adapter (module tái sử dụng)

**File mới `src/processing/softdelete.py`** — độc lập dự án, chỉ phụ thuộc một
connection có `.execute()` (cùng ranh giới `SqlConnection` mà
`autoloader/engine.py` đã dùng).

```python
@dataclass(frozen=True)
class SoftDeleteConfig:
    target: str
    business_key: tuple[str, ...]
    key_source_sql: str              # SQL liệt kê key hiện có ở NGUỒN
    active_column: str = "is_active"
    deactivated_at_column: str = "_deactivated_at"
    max_deactivation_ratio: float = 0.1

@dataclass(frozen=True)
class SoftDeleteResult:
    source_keys: int
    deactivated: int
    reactivated: int

def apply_soft_delete(sql, config, *, now) -> SoftDeleteResult
```

Hai thao tác, cả hai idempotent:
1. **Deactivate** — có ở target, không có ở nguồn → `is_active = FALSE`
2. **Reactivate** — đang tắt nhưng nguồn có lại → `is_active = TRUE`

(2) là phần **thêm so với ảnh**: không có nó thì một lần seed lỗi tắt vĩnh viễn
các dòng đúng, không có đường quay lại ngoài sửa tay.

**Hai guard — cũng không có trong ảnh, nhưng là chỗ pattern này hỏng nặng nhất:**
- `key_source_sql` trả **0 dòng** → raise. Nguồn hỏng/JDBC rớt sẽ tắt sạch bảng
  mà không báo gì.
- Vượt `max_deactivation_ratio` → raise. 126 phường mất 1–2 là nghị quyết; mất
  60 là nguồn sai.

Khai báo trong process YAML, đúng phong cách YAML-driven của framework:
```yaml
soft_delete:
  - target: gold.dim_ward
    business_key: [ward_code]
    key_source_sql: |
      SELECT commune_code FROM catalog1.seed.ward_coordinates_seed
      WHERE province_code = '01'
```

`run_process` chạy soft delete **sau khi dbt exit 0, trước khi advance
checkpoint**. Soft delete lỗi ⇒ run FAILED ⇒ checkpoint không nhích.

**Model phải đổi kèm:** `dim_ward` và `bridge_ward_grid` từ `table` sang
`incremental` (`unique_key` = `ward_code` / `ward_code+weather_model`), nếu không
`CREATE OR REPLACE` xoá sạch cờ mỗi lần build. Cả hai giữ dòng phường đã giải thể
để `fct_ward_rain_daily` không mất lịch sử; fact mang thêm `ward_is_active`.

**Giới hạn ghi rõ trong doc:** không làm validity window (SCD2) — biết phường
đang inactive, không biết giải thể vào *ngày* nào.

## Phase 4 — Metrics

Ảnh có "Calculate Metrics: Insert/Update Count". dbt của ta **không** cung cấp
được: `transform/target/run_results.json` chỉ có 2/84 node mang `rows_affected`
(hai seed), vì `main` statement của materialization DuckLake là
`DROP TABLE gold_inc_slice`.

Làm cái rẻ và có giá trị thật thay vì bám ảnh: thêm `target_row_count BIGINT` vào
`processing.processing_runs`, ghi bằng một `COUNT(*)` trên target sau khi thành
công. Đủ để bắt "run xanh nhưng bảng rỗng" — thứ mà insert/update count sinh ra
để bắt. Soft delete trả `deactivated`/`reactivated` ghi cùng chỗ.

**Sửa:** `src/processing/schema.py`, `state.py` (`complete_run`), `runner.py`.

---

## Verification

```bash
uv run pytest tests/unit -q && uv run ruff check .
```

**Phase 1** — một catalog, ingest lại sạch:
```bash
uv run python scripts/reset_lakehouse.py --yes --reset-ingestion --reset-processing
uv run python scripts/bootstrap.py && uv run load-sources
```
Kỳ vọng: 1.294 file / 19,9M dòng / 0 lỗi; `bronze/` trên MinIO chỉ còn `files/`.

**Phase 2** — hai tiêu chí đạt/không đạt:
```bash
make transform && make transform      # chạy hai lần liên tiếp
```
1. Lần thứ hai (không dữ liệu mới) bước tìm cửa sổ **giảm từ ~49s xuống <1s**.
2. Sau lần thứ hai, `SELECT COUNT(*) FROM silver.weather_hourly WHERE _updated_at
   > <run_started_at lần 2>` phải trả **0** — chứng minh MERGE change-aware
   không bump dòng không đổi. Đây là test quan trọng nhất của cả plan.

**Phase 3** — mô phỏng phường giải thể mà không sửa seed: test đơn vị dựng target
giả 5 dòng, bỏ 1 khỏi nguồn, khẳng định `deactivated=1`; chạy lại `deactivated=0`
(idempotent); nguồn rỗng → raise; vượt ngưỡng → raise.

## Ngoài phạm vi

Forecast Gold, climatology 1991–2020, drought/anomaly/event, điểm ngập QĐ 2280,
OSM, mực nước sông, SCD2 ranh giới phường, registry `processing.assets`.

## Cần biết trước khi bắt đầu

Phase 1 chạm ~17 file và làm hỏng mọi thứ cho tới khi xong — làm trọn trong một
commit, verify bằng ingest lại rồi mới sang Phase 2.

70% raw và quota API đã tiêu cho dữ liệu trùng: cửa sổ fetch backfill chồng nhau.
Ở tầng staging đó là change log hợp lệ, nhưng ở tầng fetch là lãng phí thật —
nên xem lại `fetch-archive` trước đợt backfill tiếp theo. Không sửa trong plan này.
