# dbt transform

Project dbt cho Hanoi Flood & Climate Risk Monitor. DuckDB thực thi SQL; các
model Silver/Gold được lưu trong DuckLake (Postgres metadata, MinIO Parquet).

## Rainfall forecast KPI

- `silver.int_weather_forecast_hourly`: lịch sử các retrieval run hoàn chỉnh,
  grain `forecast_run_id × grid_cell_id × valid_time_utc`.
- `gold.bridge_ward_grid`: centroid phường → returned forecast grid gần nhất.
- `gold.fct_rain_forecast_hourly`: lịch sử forecast-vintage, rolling
  trailing 1/3/6/12/24 giờ và forward `forecast_next_*h_mm` để đọc
  lượng mưa sau timestamp hiện tại về phía trước; band QĐ 2280 và dải mưa QĐ 18.
- `gold.fct_rain_forecast_current_hourly`: view serving của run mới nhất, chỉ
  giữ các giờ chưa hết hạn.
- `gold.fct_rain_pressure_alert`: tín hiệu áp lực mưa theo phường × giờ, gồm
  mức `UNKNOWN/NORMAL/WATCH/ELEVATED/HIGH`, điểm 0–100 khi có input,
  lý do kích hoạt, persistence
  giữa ba forecast run gần nhất và revision 24 giờ. Đây là heuristic vận hành,
  không phải xác suất ngập hay cảnh báo chính thức.

## Model inventory hiện hành

Nguồn duy nhất cho archive là `silver.int_weather_archive_hourly`; mọi grain và
window đều giữ `weather_model`, vì ERA5 và ECMWF IFS không phải một chuỗi đồng
nhất. Tên dưới đây khớp trực tiếp với các file trong `transform/models/`.

| Nhóm | Model | Grain / mục đích |
|---|---|---|
| Geography | `gold.dim_grid` | model × sản phẩm × ô lưới |
| Geography | `gold.dim_ward` | 126 phường/xã, SCD active flag |
| Geography | `gold.bridge_ward_grid` | phường ↔ ô forecast/archive |
| Archive | `gold.fct_rain_archive_hourly` | grid × giờ, rolling 1–24h và bands |
| Forecast | `gold.fct_rain_forecast_hourly` | forecast run × grid × giờ, trailing + forward windows |
| Forecast | `gold.fct_rain_forecast_current_hourly` | view run mới nhất, horizon chưa hết hạn |
| Forecast | `gold.fct_rain_pressure_alert` | run × phường × giờ, pressure level/score/revision |
| Flood reference | `gold.dim_flood_point` | danh mục điểm ngập dùng cho map/replay |
| Flood observation | `gold.fct_flood_event_observation` | ghi nhận đã xác minh dùng để đối chiếu replay |

### Quy ước completeness

- Rolling hourly chỉ có giá trị khi đủ đúng số giờ có `precipitation_mm`; gap hay
  `NULL` không được thay bằng 0.
- Gold chỉ materialize relation có consumer. Dashboard chiếu archive hourly
  sang phường tại query-time.

### Archive replay

Dashboard chọn ngày/giờ trực tiếp từ `fct_rain_archive_hourly` và có thể đối
chiếu `fct_flood_event_observation`. Ghi nhận báo chí không phải bộ nhãn đầy đủ
và không được dùng để phát hành xác suất hay độ sâu ngập.

## Chạy và kiểm tra

Từ thư mục repository:

```bash
make transform             # Silver + Gold archive
make transform-forecast    # Silver + Gold forecast
```

Khi business rule/schema của model incremental thay đổi, chạy migration
full-refresh qua processing framework để giữ audit và checkpoint an toàn:

```bash
make processing-full-refresh PROCESS=rain_gold \
  REASON='describe archive schema change'
make processing-full-refresh PROCESS=forecast_gold \
  REASON='describe forecast schema change'
```

Hoặc từ thư mục `transform/`:

```bash
uv run dbt build --profiles-dir .
```

Chỉ build Gold archive contract:

```bash
uv run dbt build --profiles-dir . --select fct_rain_archive_hourly
```
