/* Ward -> weather grid cell, one row per ward and weather model. */

{{ config(materialized = 'table', tags = ['bridge']) }}

SELECT
    ward_code,
    weather_model,
    grid_cell_id,
    grid_elevation_m,
    COUNT(*) OVER (PARTITION BY weather_model, grid_cell_id) AS ward_count_on_grid
FROM {{ ref('stg_seed__ward_grid') }}
