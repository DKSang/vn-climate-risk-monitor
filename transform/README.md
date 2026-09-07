# dbt transform

Project dbt cho Hanoi Flood & Climate Risk Monitor. DuckDB thực thi SQL; các
model Silver/Gold được lưu trong DuckLake (Postgres metadata, MinIO Parquet).

## Rainfall forecast KPI

- `silver.int_weather_forecast_hourly`: lịch sử các retrieval run hoàn chỉnh,
  grain `forecast_run_id × grid_cell_id × valid_time_utc`.
- `gold.bridge_ward_grid`: centroid phường → returned forecast grid gần nhất.
- `gold.fct_rain_forecast_hourly`: lịch sử forecast-vintage, rolling
  1/3/6/12/24/48/72 giờ, band QĐ 2280
  và dải mưa QĐ 18.
- `gold.fct_rain_forecast_current_hourly`: view serving của run mới nhất, chỉ
  giữ các giờ chưa hết hạn.
- `gold.fct_flood_risk_score`: projection forcing từ grid sang 126 phường.

## Historical và climate KPI

Nguồn duy nhất là `silver.archive_hourly`; IFS chỉ được dùng khi có thật trong
relation này. Mọi grain, window, join climatology và projection đều giữ
`weather_model`, vì ERA5 và ECMWF IFS không phải một chuỗi đồng nhất.

| Model | Grain / mục đích |
|---|---|
| `gold.dim_grid` | `weather_model` × `weather_product` × lat6/lon6; fact chỉ giữ `grid_cell_id` |
| `gold.fct_rainfall_historical_hourly` | `grid_cell_id` × giờ; rolling 1/3/6/12/24/48/72h, band QĐ 2280 và QĐ 18 |
| `gold.fct_rainfall_historical_daily` | `grid_cell_id` × ngày UTC; daily rainfall chỉ có khi đủ 24 giờ |
| `gold.fct_rainfall_climatology_monthly` | `grid_cell_id` × tháng × window × `all_available_by_model_v1` |
| `gold.fct_rainfall_historical_anomaly_hourly` | anomaly so với median tháng và cờ vượt p95/p99 |
| `gold.fct_rainfall_drought_daily` | tích lũy mưa 30/60/90 ngày tại grid |
| `gold.fct_ward_rainfall_drought_daily` | projection tích lũy 30/60/90 ngày sang ward |
| `gold.fct_rainfall_historical_event` | event total/duration/peak 1h/3h/time-to-peak |
| `gold.dim_historical_replay_window` | khai báo version-controlled các cửa sổ replay |
| `gold.fct_ward_historical_replay_hourly` | rolling KPI trong replay window, projection sang ward |

### Quy ước completeness và baseline

- Rolling hourly chỉ có giá trị khi đủ đúng số giờ có `precipitation_mm`; gap hay
  `NULL` không được thay bằng 0. Daily và rolling 30/60/90 ngày dùng cùng nguyên
  tắc.
- Climatology dùng toàn bộ archive **sẵn có của từng model** theo cùng grid và
  calendar month. Đây là empirical operational baseline, không được gọi là WMO
  30-year normal. `observation_count`, khoảng thời gian baseline và version được
  công bố; anomaly/exceedance chỉ có khi baseline có ít nhất 100 quan sát.
- `rain_*_exceeds_p95/p99` là cờ vượt phân vị lượng mưa model (`>`), không phải
  xác suất hoặc dự báo ngập.
- Tích lũy 30/60/90 ngày là rainfall input phục vụ theo dõi hạn, không phải một
  drought class/index độc lập.

### Event rainfall

Định nghĩa `wet_gt_0_1mm_six_dry_hours_v1` dùng `wet > 0.1 mm`; event mới bắt đầu
sau ít nhất 6 giờ khô liên tiếp có quan sát. Giờ thiếu/`NULL` tách observed run,
không bị coi là giờ khô. Event giữ total, elapsed duration từ giờ wet đầu đến giờ
wet cuối, peak 1h/3h, time-to-peak và left/right censor flags. Quy tắc 6 giờ là
version v1 cần hiệu chỉnh, không phải chuẩn ngập Hà Nội.

### Historical replay

Biên cửa sổ là UTC nửa mở `[window_start_utc, window_end_utc)`:

| `replay_window_id` | Cửa sổ UTC |
|---|---|
| `hanoi_historic_rain_2008` | 2008-10-30 → 2008-11-03 |
| `typhoon_yagi_2024` | 2024-09-06 → 2024-09-13 |
| `late_august_heavy_rain_2025` | 2025-08-25 → 2025-08-29 |
| `widespread_flood_2025_10_08` | 2025-10-07 → 2025-10-10 |

Các cửa sổ là scenario case được khai báo rõ để replay model rainfall. Chúng
không phải bộ nhãn ngập đầy đủ, không xác nhận tổng mưa trạm tại từng ward và
không được dùng để phát hành `flood_probability` hay `flood_depth`.

## Chạy và kiểm tra

Từ thư mục repository:

```bash
make transform
```

Khi business rule/schema của model incremental thay đổi, chạy migration
full-refresh qua processing framework để giữ audit và checkpoint an toàn:

```bash
make processing-full-refresh PROCESS=rain_gold \
  SELECT='fct_rain_archive_hourly+' REASON='rain band v2'
```

Khi triển khai migration thêm `forecast_run_id`, full-refresh cả hai process
forecast một lần; các lượt hourly sau đó quay lại incremental:

```bash
make processing-full-refresh PROCESS=forecast_silver \
  REASON='migrate forecast to vintage history'
make processing-full-refresh PROCESS=forecast_gold \
  REASON='migrate forecast to vintage history'
```

Hoặc từ thư mục `transform/`:

```bash
uv run dbt build --profiles-dir .
```

Chỉ build DAG historical/climate cùng upstream dependencies:

```bash
uv run dbt build --profiles-dir . --select +tag:historical
```
