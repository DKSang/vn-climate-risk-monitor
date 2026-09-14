-- Curated grain is (model, grid cell, hour).
SELECT grid_cell_id, valid_time_utc, COUNT(*) AS rows_at_grain
FROM {{ ref('int_weather_archive_hourly') }}
GROUP BY grid_cell_id, valid_time_utc
HAVING COUNT(*) > 1
