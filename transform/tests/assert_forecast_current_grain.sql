-- Current serving view must keep one row per forecast run × grid × hour.
SELECT forecast_run_id, grid_cell_id, valid_time_utc, COUNT(*) AS rows_at_grain
FROM {{ ref('fct_rain_forecast_current_hourly') }}
GROUP BY forecast_run_id, grid_cell_id, valid_time_utc
HAVING COUNT(*) > 1
