-- Each run/grid needs a complete forward horizon from its first hour.
WITH first_hour AS (
    SELECT
        forecast_run,
        grid_cell_id,
        forecast_next_24h_mm,
        ROW_NUMBER() OVER (
            PARTITION BY forecast_run, grid_cell_id
            ORDER BY valid_at
        ) AS row_number
    FROM {{ ref('fct_rain_forecast_hourly') }}
)
SELECT *
FROM first_hour
WHERE row_number = 1
  AND forecast_next_24h_mm IS NULL
