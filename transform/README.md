# dbt transform

Project dbt cho Hanoi Flood & Climate Risk Monitor. DuckDB thực thi SQL; các
model Silver/Gold được lưu trong DuckLake (Postgres metadata, MinIO Parquet).

## Phase 5 rainfall MVP

- `silver.forecast_hourly`: snapshot forecast hoàn chỉnh mới nhất, không trộn
  logical retrieval slot.
- `silver.bridge_hanoi_ward_forecast_grid`: centroid phường → returned forecast
  grid gần nhất trong snapshot.
- `gold.fct_rainfall_forecast_hourly`: rolling 1/3/6/12/24/48/72 giờ.
- `gold.fct_rainfall_forecast_summary`: totals và peaks trong horizon tương lai.
- hai model `gold.fct_ward_*`: projection forcing từ grid sang 126 phường.

Baseline lịch sử, forecast-vintage backtest và calibration với nhãn ngập chưa
thuộc MVP này. Bronze vẫn giữ source objects để triển khai về sau.

## Chạy và kiểm tra

Từ thư mục repository:

```bash
make transform
```

Hoặc từ thư mục `transform/`:

```bash
uv run dbt build --profiles-dir .
```
