/* Ward → grid mapping captured from Open-Meteo snap results. */

{{ config(materialized = 'view') }}

SELECT
    -- CSV type inference would drop the leading zeros of ward_code.
    LPAD(CAST(ward_code AS VARCHAR), 5, '0') AS ward_code,
    model AS weather_model,
    {{ grid_cell_id(
        'model',
        'ROUND(grid_latitude, 6)',
        'ROUND(grid_longitude, 6)'
    ) }} AS grid_cell_id,
    ROUND(grid_latitude, 6) AS grid_latitude,
    ROUND(grid_longitude, 6) AS grid_longitude,
    elevation_m AS grid_elevation_m
FROM {{ ref('ward_grid_map_seed') }}
