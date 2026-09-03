{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'daily']) }}

WITH daily AS (
    SELECT
        grid_cell_id,
        weather_model,
        weather_product,
        CAST(valid_time_utc AT TIME ZONE 'UTC' AS DATE) AS rainfall_date,
        SUM(precipitation_mm) AS daily_rainfall_sum_raw,
        COUNT(precipitation_mm) AS observed_hours,
        MIN(valid_time_utc) AS first_observation_utc,
        MAX(valid_time_utc) AS last_observation_utc
    FROM {{ ref('archive_hourly') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    MD5(CONCAT_WS('|', grid_cell_id, CAST(rainfall_date AS VARCHAR)))
        AS historical_daily_key,
    grid_cell_id,
    weather_model,
    weather_product,
    rainfall_date,
    CAST(DATE_PART('month', rainfall_date) AS INTEGER) AS calendar_month,
    CASE WHEN observed_hours = 24 THEN daily_rainfall_sum_raw END AS daily_rainfall_mm,
    LEAST(observed_hours / 24.0, 1.0) AS coverage_ratio,
    observed_hours = 24 AS is_complete,
    observed_hours,
    first_observation_utc,
    last_observation_utc
FROM daily
