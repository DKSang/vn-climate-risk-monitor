# Bước 6 — Storage và serving contract

**Hanoi Flood & Climate Risk Monitor** · v2.2 · 2026-09-08

Gold DuckLake là single source of truth nghiệp vụ. MinIO `bronze/files/` giữ
payload nguồn bất biến; PostgreSQL giữ metadata DuckLake và control plane.

## 1. Layer contract

```text
MinIO bronze/files/                  raw source objects
catalog1.silver.stg_*                append-only staging do autoloader ghi
catalog1.silver.int_*                dedup/conform, change-aware MERGE
catalog1.gold.*                      relations có consumer rõ ràng
PostgreSQL ingestion.*              file checkpoint và lease
PostgreSQL processing.*             processing checkpoint và audit run
```

Không copy Gold sang PostgreSQL và không glob Parquet trực tiếp. Mọi consumer
đọc qua catalog DuckLake để tôn trọng snapshot metadata.

## 2. Gold inventory

| Relation | Grain / vai trò |
|---|---|
| `dim_grid` | weather model/product × grid |
| `dim_ward` | 126 phường/xã, soft delete bằng `is_active` |
| `dim_flood_point` | danh mục điểm úng dùng cho map/replay |
| `bridge_ward_grid` | ward × weather model → grid |
| `fct_flood_event_observation` | ghi nhận ngập đã xác minh cho replay |
| `fct_rain_archive_hourly` | grid × giờ archive |
| `fct_rain_forecast_hourly` | forecast run × grid × valid time |
| `fct_rain_forecast_current_hourly` | view run mới nhất, chỉ giờ chưa hết hạn |
| `fct_rain_pressure_alert` | forecast run × ward × valid time |

Không materialize relation khi chưa có consumer. Projection archive từ grid
sang ward được thực hiện lúc query qua bridge nhỏ.

## 3. Canonical grid

Tọa độ được round 6 chữ số ở Silver trước khi tạo khóa:

```text
grid_cell_id = MD5(weather_model | weather_product | lat6 | lon6)
```

Fact chỉ giữ `grid_cell_id`; latitude, longitude và elevation thuộc
`dim_grid`. ERA5 archive, ECMWF IFS archive và ECMWF IFS forecast là các product
khác nhau, không được nối thành một chuỗi khí hậu duy nhất.

## 4. Processing checkpoint

`processing.processing_state.last_successful_start_at` là start time của run
thành công gần nhất. Mỗi process đọc lại từ checkpoint trừ `safety_lag`; chỉ
advance checkpoint sau khi dbt exit thành công.

| Process | Source watermark | Gold selector |
|---|---|---|
| `rain_gold` | `int_weather_archive_hourly._updated_at` | archive serving contract tường minh |
| `forecast_gold` | `int_weather_forecast_hourly._updated_at` | forecast/current/pressure tường minh |

Không dùng `MAX(timestamp)` của Gold làm checkpoint. Khi cần rewind, dùng
`scripts/run_processing.py reprocess-from` để tạo audit record thay vì sửa
control table bằng tay.

## 5. Incremental và snapshot

- Intermediate weather tables dùng change-aware MERGE theo `_row_hash`.
- Hai history fact hourly dùng incremental key và recompute đủ lookback/window.
- `fct_rain_forecast_current_hourly` là view; history không bị prune.
- Dimension/bridge mutable giữ `is_active` để fact lịch sử không thành orphan.
- DuckLake snapshot giữ 7 ngày; file đã hết snapshot chờ thêm 2 ngày trước cleanup.
- Maintenance chạy ở DAG riêng, không chạy trong dbt hook.

## 6. Retention

- Archive raw/staging/history: giữ dài hạn cho replay.
- Forecast raw/staging/history: giữ mọi logical run để audit revision và
  persistence.
- Current view và pressure mart có thể tái tạo từ forecast history.
- Relation bị drop còn có thể khôi phục qua snapshot cho đến khi maintenance
  expire snapshot và cleanup file.

## 7. Definition of Done

- Gold inventory khớp model có consumer trong code.
- Pipeline dùng selector tường minh, không dùng `tag:marts` kéo model ngoài scope.
- Dbt tests xác nhận grain, relationship, forward coverage và pressure class.
- Mỗi Gold run thành công lưu `published_snapshot_id` cùng transaction với
  checkpoint; dashboard chỉ đọc snapshot đã publish, không đọc catalog HEAD có
  thể đang ở giữa một build nhiều model.
- Không có relation hoặc tài liệu gọi pressure score là xác suất ngập/cảnh báo
  chính thức.

## 8. Backup và recovery

DuckLake gồm hai phần không thể thay thế nhau: PostgreSQL giữ catalog/control
plane, MinIO giữ raw object và Parquet. `backup-metadata` chỉ phù hợp khi cần bảo
vệ metadata; disaster recovery phải dùng `backup-lakehouse` khi mọi writer đã
dừng:

```bash
LAKEHOUSE_BACKUP_ROOT=/mnt/backup \
BACKUP_QUIESCED=1 \
make backup-lakehouse
```

MinIO Client alias mặc định là `lakehouse`; có thể đổi bằng
`MC_SOURCE_ALIAS`. Restore yêu cầu `RESTORE_QUIESCED=1` và xác nhận đúng tên
bucket để tránh ghi nhầm. Sau restore phải chạy `make quality`, bao gồm test đọc
file thật `assert_gold_is_readable`. Trước restore, chạy
`LAKEHOUSE_BACKUP_DIR=... make verify-lakehouse-backup`; verifier kiểm checksum
của PostgreSQL dump và từng object MinIO mà không thay đổi đích.
