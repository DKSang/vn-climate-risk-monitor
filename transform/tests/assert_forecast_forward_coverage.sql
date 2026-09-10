-- Mỗi run/grid phải có đủ cửa sổ nhìn về phía trước ngay tại giờ đầu tiên.
-- Nếu không, pressure alert sẽ vô tình phân loại dựa trên horizon bị cụt.
WITH first_hour AS (
    SELECT
        forecast_run_id,
        grid_cell_id,
        forecast_next_24h_mm,
        ROW_NUMBER() OVER (
            PARTITION BY forecast_run_id, grid_cell_id
            ORDER BY valid_time_utc
        ) AS row_number
    FROM {{ ref('fct_rain_forecast_hourly') }}
)
SELECT *
FROM first_hour
WHERE row_number = 1
  AND forecast_next_24h_mm IS NULL
