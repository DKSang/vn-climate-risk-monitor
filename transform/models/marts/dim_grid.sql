/* Weather-model grid cells, as mapped by scripts/map_ward_grid.py. */

{{ config(materialized = 'table', tags = ['dim']) }}

SELECT
    grid_cell_id,
    weather_model,
    grid_latitude,
    grid_longitude,
    MEDIAN(grid_elevation_m) AS elevation_m,
    COUNT(*) AS ward_count
FROM {{ ref('stg_seed__ward_grid') }}
GROUP BY grid_cell_id, weather_model, grid_latitude, grid_longitude
