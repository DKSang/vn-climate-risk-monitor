# Bước 6 — Lưu trữ: single source of truth

> **Đã thay thế một phần (2026-09-03).** Kiến trúc chốt hiện tại ở
> [plan lean medallion](superpowers/plans/2026-09-03-lean-medallion.md) và
> [plan Silver Layer Flow](superpowers/plans/2026-09-03-silver-layer-flow.md):
> MỘT catalog DuckLake (`catalog1`), Bronze chỉ còn là landing zone raw file,
> bảng append-only của autoloader nay là `silver.stg_*`.
> Phần mô tả `bronze_store` / hai catalog / các model gold cũ trong tài liệu
> này KHÔNG còn đúng.

**Hanoi Flood & Climate Risk Monitor** · 2026-08-31

**Trạng thái:** đã triển khai 2026-08-31, tái cấu trúc lean medallion & Silver Layer Flow 2026-09-03.
**Cập nhật 2026-09-03:**
- Checkpoint Gold chuyển sang `processing.processing_state` (start time của lần chạy thành công gần nhất). `ingestion.gold_watermarks` deprecated — xem [plan 2026-09-03](superpowers/plans/2026-09-03-processing-checkpoint.md).
- Cấu trúc dbt chuẩn hoá 3 lớp: `staging/` (view mỏng), `intermediate/` (`int_weather_archive_hourly` — incremental table MERGE change-aware), `marts/` (table/incremental).
- Fact/dim tinh gọn: `dim_grid`, `dim_ward`, `bridge_ward_grid`, `fct_rain_archive_hourly`, `fct_rain_archive_daily`, `fct_ward_rain_archive_daily`.

Gold DuckLake là SSOT nghiệp vụ. Bronze files là SSOT payload nguồn. Không copy
Gold sang Postgres. Bước 8 đọc DuckLake.

## 1. Quyết định đã khóa

| # | Quyết định |
|---|---|
| Phạm vi | Star + dim tinh gọn cho nhu cầu hiện hành. Không climatology/event chưa dùng. |
| Điểm úng | Có. Thu thập QĐ 2280 → CSV review. Không bịa tọa độ. |
| `dim_grid` | Grain `(weather_model, lat6, lon6)`. `weather_model ∈ {era5, ecmwf_ifs}`. |
| Tọa độ | Round/canonicalize **6 chữ số trước** khi tạo `grid_cell_id` và trước mọi mapping. |
| Phường | Soft delete qua `is_active` (`dim_ward` và `bridge_ward_grid`). |
| SSOT | `catalog1.gold.*`. Bronze files = payload. |
| Checkpoint Gold | `processing.processing_state.last_successful_start_at` = **start time** của run SUCCEEDED gần nhất, trừ `safety_lag`. Không dùng `MAX()` của bất kỳ timestamp nào làm checkpoint. |
| Autoloader | Chỉ Bronze/staging. File: `PENDING → PROCESSING → COMMITTED` (hoặc `FAILED`). Chỉ `COMMITTED` được coi là durable. |
| Incremental | Hourly merge theo watermark control `_updated_at`. |
| Fact | Chỉ `grid_cell_id`. Lat/lon ô chỉ ở `dim_grid`. |
| Staging | Bảng append-only do autoloader ghi (`stg_weather_*`) + dbt view mỏng. |
| Curated / Inter | Incremental table (`int_weather_archive_hourly`), MERGE change-aware, watermark `_updated_at`. |
| Marts | Table (mặc định), dim/bridge incremental để giữ cờ soft-delete. |

Ngoài phạm vi: API/SMI/IDF, xác suất ngập, serving, SCD2 hành chính trước 2025.

## 2. Medallion

```text
Landing     bronze/files/...                 JSON bất biến (MinIO)
Staging     catalog1.silver.stg_*            table append-only (autoloader) + view dbt (stg_*)
Curated     catalog1.silver.int_*            table incremental: dedup + MERGE change-aware
Gold        catalog1.gold.*                  table / incremental — SSOT nghiệp vụ
Control     PostgreSQL ingestion + processing
```

Silver **không** expire snapshot vì không có file Parquet riêng.

## 3. Canonical tọa độ

Mọi lat/lon dùng cho khóa hoặc mapping đi qua một quy tắc, ở **Silver view**
(và seed map-grid khi sinh CSV):

```text
lat6 = ROUND(grid_latitude,  6)
lon6 = ROUND(grid_longitude, 6)
```

```text
grid_cell_id = MD5(
  weather_model || '|' || weather_product || '|' ||
  PRINTF('%.6f', lat6) || '|' || PRINTF('%.6f', lon6)
)
```

Không round lần hai ở Gold. `dim_grid` lưu `lat6`/`lon6` đã canonicalize.

## 4. Protocol commit — bốn trạng thái phải tách

```text
PENDING ──► PROCESSING ──► COMMITTED
                 └──────► FAILED ──► (retry → PROCESSING)
```

- Không có trạng thái `ENDING`. Trạng thái thành công duy nhất của file là `COMMITTED`.
- Chỉ file `COMMITTED` được phép đẩy logical checkpoint Auto Loader (file đó không nạp lại).
- File **không** được `COMMITTED` trước khi INSERT Bronze **durable** (DuckLake commit xong).
- Fail: `PENDING` vẫn `PENDING` (hoặc `PROCESSING` hết lease → claim lại). Không đụng Gold watermark.

### 4.1 Ý nghĩa từng commit

| Sự kiện | Ý nghĩa | Không có nghĩa |
|---|---|---|
| Bronze DuckLake commit | Bảng Bronze đã có dòng của file | Gold đã cập nhật; file đã checkpoint |
| `ingestion_files.status = COMMITTED` | Bronze của file đó durable; Auto Loader không nạp lại file | Gold đã merge; watermark Gold đã tăng |
| Gold DuckLake commit | Bảng Gold hourly đã ghi slice này | Checkpoint control đã tăng |
| `processing_runs` SUCCEEDED | Mọi thứ visible **lúc run bắt đầu** đã vào Gold thành công | Mọi file COMMITTED đều đã lên Gold |

`COMMITTED` **không** kéo Gold. Khoảng trễ `COMMITTED` mà checkpoint Gold chưa tới là trạng thái bình thường giữa `make load` và `make transform-archive`.

### 4.2 Crash

```text
A. Chết sau Bronze commit, trước file COMMITTED
   Bronze: có (at-least-once, có thể trùng)
   control: PROCESSING → lease hết → PENDING
   Gold watermark: không đổi
   → retry INSERT Bronze; Silver dedup; Gold chưa chạy.

B. Chết sau file COMMITTED, trước Gold commit
   Bronze: có
   control: COMMITTED
   Gold: chưa có slice
   → ĐÚNG nếu hiểu protocol. Gold run sau đọc watermark CŨ, merge lại
     mọi Bronze `_ingested_at` > watermark. Không được coi COMMITTED = Gold xong.

C. Chết sau Gold commit, trước run SUCCEEDED
   Gold: có slice
   checkpoint: chưa tăng
   → Gold run sau merge trùng khóa (DELETE+INSERT idempotent). Checkpoint tăng khi SUCCEEDED.
```

Cấm dùng `MAX(<timestamp>)` làm checkpoint. Hai lý do, lý do thứ hai mới là lý do
thật:

1. `MAX()` không chứng minh run đã thành công.
2. `MAX()` đọc **sau** khi transform xong sẽ bao gồm cả row mà autoloader commit
   *trong lúc* transform chạy — những row đó chưa được xử lý nhưng đã bị checkpoint
   nhảy qua. Đó là bug của thiết kế 2026-08-31, sửa ở 2026-09-03.

### 4.3 Control table Gold — schema `processing`

Tách khỏi `ingestion` vì trả lời câu hỏi khác: `ingestion_files` = "file đã vào
Bronze chưa", `processing_state` = "process đã xử lý tới mốc nào".

```text
processing.processing_state          -- 1 row / (process, source, scope)
  process_key, source_ref, scope     PK
  last_successful_start_at           -- START time của run SUCCEEDED gần nhất
  last_successful_run_id

processing.processing_runs           -- audit
  processing_run_id
  process_key, scope, target_ref
  started_at_utc, completed_at_utc
  status                             -- RUNNING | SUCCEEDED | FAILED | REWIND
  bounds JSONB                       -- {source: {checkpoint_before, lower_bound}}
  checkpoint_candidate               -- = started_at_utc
  actor, reason                      -- ai rewind checkpoint và vì sao
  error_type, error_message
```

`source_ref` và `scope` nằm trong khóa: một process đọc nhiều source thì rewind
được từng source riêng, và dev/production không còn dùng chung một hàng như
`gold_watermarks` cũ.

Luồng `transform-archive` (`scripts/run_processing.py run`):

```text
1. run_started_at = CURRENT_TIMESTAMP của Postgres  ← TRƯỚC khi đọc gì
2. checkpoint_before = processing_state.last_successful_start_at
     chưa có → full refresh
3. lower_bound = checkpoint_before − safety_lag     ← KHÔNG có upper bound
4. INSERT processing_runs status=RUNNING, checkpoint_candidate=run_started_at
5. dbt build --vars {processing_bounds: {...}}
     new = silver.archive_hourly WHERE _ingested_at > lower_bound
     recompute rolling trên [min(t) − 71h, max(t) + 71h] mỗi ô
     DELETE+INSERT Gold + full refresh dependents
6. CHỈ KHI dbt exit 0: processing_state = run_started_at, run SUCCEEDED
```

Vì sao bước 3 không có chặn trên: `_ingested_at` là transaction START time của
DuckDB, còn row chỉ visible lúc COMMIT. Một row đóng dấu 09:59:58 có thể xuất
hiện sau khi run 10:00:00 đã đọc xong. Chặn trên đóng sẽ loại nó khỏi mọi cửa sổ
tương lai; `safety_lag` ở chặn dưới là thứ duy nhất cứu được, và MERGE idempotent
hấp thụ phần lặp.

Fail ở bất kỳ bước nào của 5: run `FAILED`, checkpoint không đổi, file `PENDING`
không bị đụng (chúng đã COMMITTED ở Bronze). Retry đọc lại đúng cửa sổ vừa hỏng.

### 4.4 Reprocess

Không `UPDATE processing_state` tay. Dùng lệnh có audit:

```bash
uv run python scripts/run_processing.py reprocess-from rainfall_historical_hourly \
    --from 2026-08-25T00:00:00Z --reason "fix bug X"
```

Nó ghi một row `REWIND` vào `processing_runs` kèm `actor` + `reason`, nên lịch sử
checkpoint đọc được cùng chỗ với lịch sử run.

## 5. Star schema

```text
dim_grid
  grid_cell_id PK
  weather_model          era5 | ecmwf_ifs
  grid_latitude         -- lat6
  grid_longitude        -- lon6

dim_ward                 126 phường/xã Hà Nội, soft-delete qua `is_active`
bridge_ward_grid         ward_code × weather_model → grid_cell_id, is_active, ward_count_on_grid

fct_rain_archive_hourly          grid_cell_id × valid_time_utc, rolling 1/3/6/12/24h, scenario bands, watermark _updated_at
fct_rain_archive_daily           grid_cell_id × rain_date
fct_ward_rain_archive_daily      ward_code × rain_date (chiếu fct_rain_archive_daily qua bridge_ward_grid)
```

Q4: unique `grid_cell_id`, không average phường. Chiếu về phường ở grain NGÀY (`fct_ward_rain_archive_daily`), không nhân bản ở grain giờ.

## 6. Climatology window (khóa)

`window_hours` (1/3/6/12/24/48/72) **không** phải climate window.

Climate window là seed `dim_climatology_window` / `climatology_window_seed`:

```text
climatology_window_id              all_available_by_model_v1
weather_model
weather_product                    archive
climatology_start_year             năm đầu có dữ liệu complete của model đó
climatology_end_year               năm cuối complete tại lúc build
definition                         empirical, không phải WMO 1991–2020
```

Khóa hiện tại: **`all_available_by_model_v1`**. Không im lặng đổi sang 1991–2020 (IFS không có trước 2017). `start_year`/`end_year` **ghi trên hàng climatology** mỗi lần full refresh.

Anomaly **bắt buộc** `climatology_window_id` (và `weather_model`, `weather_product`, `grid_cell_id`, `calendar_month`, `window_hours`) khớp climatology. Không join lệch window.

## 7. Incremental hourly

Thêm giờ `t` → rolling H của mọi `T ∈ [t, t+H-1]` đổi. H = 72.

Slice `new` lấy từ intermediate table `int_weather_archive_hourly` với `_updated_at > last_successful_start_at` (qua macro `incremental_changed_filter`), **không** từ `MAX` trên Gold.

Macro incremental: `DELETE`+`INSERT` vào tên đích, không `__dbt_tmp`.

## 8. SCD2 phường — chỉ hiện tại → tương lai

Không reconstruct ranh giới trước 2025. Mapping ward code ↔ polygon × tên × hiệu lực pháp lý theo từng thời kỳ **không** thuộc bước 6.

- `valid_from_utc = 2025-07-01` (NQ 1656)
- `valid_to_utc = NULL`, `is_current = true`
- Version mới khi có nghị quyết **sau** mốc này (đóng version cũ, mở version mới)
- Replay 2008/2024/2025 chiếu lên ranh giới 2025; cột `as_of_boundary_version = nq_1656_2025`

`ward_code` unique khi mới một version; unique `(ward_code, valid_from_utc)` khi có version sau.

## 9. Điểm úng

CSV `transform/seeds/hanoi_flood_points_seed.csv` — lat/lon round 6 số **trước** map.

Cổng: chưa review thì không fact. Map nearest `dim_grid` theo `(weather_model, weather_product)` trên tọa độ đã canonicalize.

## 10. Retention

- Bronze files + Bronze tables: giữ, append.
- Gold snapshots: expire 7 ngày (`on-run-end`).
- Silver: view, không snapshot.
- Không TTL JSON.

## 11. Definition of Done

- Contract này khớp code.
- Tọa độ canonicalize ở Silver trước key/mapping.
- `dim_grid` có `weather_model` + `weather_product`; forecast = `ecmwf_ifs`/`forecast`.
- Fact không lat/lon ô.
- Gold watermark chỉ tăng sau Gold SUCCEEDED; test crash-B: COMMITTED Bronze + watermark cũ → retry Gold bù.
- File không COMMITTED trước Bronze durable (hành vi Auto Loader hiện tại, không nới).
- Climatology + anomaly cùng `climatology_window_id`.
- SCD2 từ 2025-07-01; không dataset GitHub lịch sử.
- Điểm úng khi có CSV. `dbt build` xanh. Không `flood_probability`.

## 12. Rủi ro

| # | Rủi ro | Xử lý |
|---|---|---|
| R3 | Phụ lục điểm PDF | Cổng CSV |
| R-boundary | Replay ≠ ranh giới năm đó | Ghi `as_of_boundary_version` |
| R-wm | Nhầm COMMITTED = Gold xong | Hai control table; §4 |
| R-inc | `__dbt_tmp` | Macro DELETE+INSERT |
| R1 | Nhiều điểm một ô | Cùng forcing |
