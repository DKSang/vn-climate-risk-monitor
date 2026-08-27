-- Một snapshot grid-hour chỉ được xuất hiện đúng một lần.
SELECT
    forecast_snapshot_id,
    grid_cell_id,
    valid_time_utc,
    COUNT(*) AS duplicate_count
FROM {{ ref('forecast_hourly') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
