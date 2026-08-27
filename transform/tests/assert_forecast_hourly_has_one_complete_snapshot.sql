-- Empty hoặc nhiều logical slot cùng lọt vào view đều là lỗi snapshot selection.
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT forecast_snapshot_id) AS snapshot_count
FROM {{ ref('forecast_hourly') }}
HAVING COUNT(*) = 0 OR COUNT(DISTINCT forecast_snapshot_id) <> 1
