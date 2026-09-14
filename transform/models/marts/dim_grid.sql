/* Weather grid dimension; facts chỉ giữ grid_cell_id. */

{{ config(
    materialized = 'table',
    tags = ['dim', 'forecast', 'archive']
) }}

WITH observations AS (
    SELECT
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc
    FROM {{ ref('int_weather_archive_hourly') }}

    UNION ALL

    SELECT
        {{ grid_cell_id("'ecmwf_ifs_fc'", 'ROUND(grid_latitude, 6)', 'ROUND(grid_longitude, 6)') }} AS grid_cell_id,
        'ecmwf_ifs_fc' AS weather_model,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc
    FROM {{ source('silver_staging', 'stg_weather_forecast') }}
),

observed AS (
    SELECT
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        MIN(valid_time_utc) AS first_observed_utc,
        MAX(valid_time_utc) AS last_observed_utc,
        COUNT(*) AS observation_hours
    FROM observations
    GROUP BY grid_cell_id, weather_model, grid_latitude, grid_longitude
),

-- Elevation lấy từ mapping seed vì hourly response phụ thuộc requested point.
elevation AS (
    SELECT grid_cell_id, MEDIAN(grid_elevation_m) AS elevation_m
    FROM {{ ref('stg_seed__ward_grid') }}
    GROUP BY grid_cell_id
)

SELECT
    observed.*,
    elevation.elevation_m
FROM observed
LEFT JOIN elevation USING (grid_cell_id)
