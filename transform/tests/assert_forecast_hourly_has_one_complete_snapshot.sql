-- View phải chứa đúng một run và tạo thành ma trận grid × valid hour đầy đủ.
WITH snapshot_summary AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT forecast_snapshot_id) AS snapshot_count,
        COUNT(DISTINCT grid_cell_id) AS grid_count,
        COUNT(DISTINCT valid_time_utc) AS horizon_hour_count
    FROM {{ ref('forecast_hourly') }}
),

grid_horizons AS (
    SELECT
        forecast_snapshot_id,
        grid_cell_id,
        COUNT(DISTINCT valid_time_utc) AS horizon_hour_count
    FROM {{ ref('forecast_hourly') }}
    GROUP BY forecast_snapshot_id, grid_cell_id
),

hour_grid_counts AS (
    SELECT
        forecast_snapshot_id,
        valid_time_utc,
        COUNT(DISTINCT grid_cell_id) AS grid_count
    FROM {{ ref('forecast_hourly') }}
    GROUP BY forecast_snapshot_id, valid_time_utc
)

SELECT summary.*
FROM snapshot_summary AS summary
WHERE summary.row_count = 0
   OR summary.snapshot_count <> 1
   OR summary.grid_count = 0
   OR summary.horizon_hour_count = 0
   OR EXISTS (
       SELECT 1
       FROM grid_horizons AS grid
       WHERE grid.horizon_hour_count <> summary.horizon_hour_count
   )
   OR EXISTS (
       SELECT 1
       FROM hour_grid_counts AS hourly
       WHERE hourly.grid_count <> summary.grid_count
   )
