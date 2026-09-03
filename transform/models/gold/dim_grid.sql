{{ config(
    materialized = 'table',
    tags = ['gold', 'dimension', 'historical']
) }}

SELECT DISTINCT
    grid_cell_id,
    weather_model,
    weather_product,
    grid_latitude,
    grid_longitude
FROM {{ ref('archive_hourly') }}

UNION

SELECT DISTINCT
    grid_cell_id,
    weather_model,
    weather_product,
    grid_latitude,
    grid_longitude
FROM {{ ref('forecast_hourly') }}
