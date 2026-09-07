-- Grain của lớp curated forecast là (grid_cell_id, valid_time_utc).
-- Giữ dự báo mới nhất cho mỗi valid_time_utc của từng ô lưới.
-- Dedup hỏng thì fct_rain_forecast_hourly nhân đôi dữ liệu.
SELECT grid_cell_id, valid_time_utc, COUNT(*) AS rows_at_grain
FROM {{ ref('int_weather_forecast_hourly') }}
GROUP BY grid_cell_id, valid_time_utc
HAVING COUNT(*) > 1
