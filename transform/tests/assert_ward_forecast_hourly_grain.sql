SELECT
    ward_key,
    forecast_snapshot_id,
    valid_time_utc,
    COUNT(*) AS duplicate_count
FROM {{ ref('fct_ward_rainfall_forecast_hourly') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
