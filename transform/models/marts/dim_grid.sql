/* Weather grid dimension; facts chỉ giữ grid_cell_id. */

{{ config(
    materialized = 'table',
    tags = ['dim', 'forecast', 'archive']
) }}

{% set archive_model = ref('int_weather_archive_hourly') %}
{% set forecast_source = source('silver_staging', 'stg_weather_forecast') %}
{% if execute %}
    {% set archive_exists = adapter.get_relation(
        database=archive_model.database,
        schema=archive_model.schema,
        identifier=archive_model.identifier
    ) %}
    {% set forecast_exists = adapter.get_relation(
        database=forecast_source.database,
        schema=forecast_source.schema,
        identifier=forecast_source.identifier
    ) %}
{% else %}
    {% set archive_exists = archive_model %}
    {% set forecast_exists = forecast_source %}
{% endif %}

WITH observations AS (
    {% if archive_exists %}
    SELECT
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc
    FROM {{ archive_model }}
    {% endif %}

    {% if archive_exists and forecast_exists %}
    UNION ALL
    {% endif %}

    {% if forecast_exists %}
    SELECT
        {{ grid_cell_id("'ecmwf_ifs_fc'", 'ROUND(grid_latitude, 6)', 'ROUND(grid_longitude, 6)') }} AS grid_cell_id,
        'ecmwf_ifs_fc' AS weather_model,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc
    FROM {{ forecast_source }}
    {% endif %}
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
