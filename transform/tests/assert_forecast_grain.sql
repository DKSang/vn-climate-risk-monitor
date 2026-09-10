-- Mỗi retrieval run giữ riêng cùng một valid_time để audit forecast vintage.
-- Dedup chỉ được loại requested point trùng returned grid trong cùng run.
SELECT forecast_run_id, grid_cell_id, valid_time_utc, COUNT(*) AS rows_at_grain
FROM {{ ref('int_weather_forecast_hourly') }}
GROUP BY forecast_run_id, grid_cell_id, valid_time_utc
HAVING COUNT(*) > 1
