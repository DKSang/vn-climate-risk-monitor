SELECT
    forecast_run_id,
    grid_cell_id,
    valid_time_utc,
    COUNT(*) AS row_count
FROM {{ ref('fct_rain_forecast_hourly') }}
GROUP BY forecast_run_id, grid_cell_id, valid_time_utc
HAVING COUNT(*) > 1
