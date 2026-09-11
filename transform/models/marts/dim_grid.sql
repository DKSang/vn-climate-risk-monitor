/* Weather grid dimension; facts chỉ giữ grid_cell_id. */

{{ config(
    materialized = 'table',
    tags = ['dim']
) }}

{% set archive = ref('int_weather_archive_hourly') %}
{% set forecast = ref('int_weather_forecast_hourly') %}
{% if execute %}
    {% set archive_exists = adapter.get_relation(
        database=archive.database,
        schema=archive.schema,
        identifier=archive.identifier
    ) is not none %}
    {% set forecast_exists = adapter.get_relation(
        database=forecast.database,
        schema=forecast.schema,
        identifier=forecast.identifier
    ) is not none %}
{% else %}
    {% set archive_exists = true %}
    {% set forecast_exists = true %}
{% endif %}

WITH observations AS (
    {% if archive_exists %}
    SELECT
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc
    FROM {{ archive }}
    {% endif %}

    {% if archive_exists and forecast_exists %}
    UNION ALL
    {% endif %}

    {% if forecast_exists %}
    SELECT
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc
    FROM {{ forecast }}
    {% endif %}

    {% if not archive_exists and not forecast_exists %}
    SELECT
        CAST(NULL AS VARCHAR) AS grid_cell_id,
        CAST(NULL AS VARCHAR) AS weather_model,
        CAST(NULL AS DOUBLE) AS grid_latitude,
        CAST(NULL AS DOUBLE) AS grid_longitude,
        CAST(NULL AS TIMESTAMPTZ) AS valid_time_utc
    WHERE FALSE
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
