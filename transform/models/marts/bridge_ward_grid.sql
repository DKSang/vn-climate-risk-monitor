/* Ward ↔ weather grid mapping by model. */
{{ config(
    materialized = 'table',
    tags = ['bridge', 'forecast', 'archive']
) }}

WITH maps AS (
    SELECT
        ward_code,
        weather_model,
        grid_cell_id,
        grid_elevation_m
    FROM {{ ref('stg_seed__ward_grid') }}
)

SELECT
    MD5(CONCAT_WS('|', map.ward_code, map.weather_model)) AS ward_grid_key,
    map.ward_code,
    map.weather_model,
    map.grid_cell_id,
    map.grid_elevation_m,
    COUNT(*) OVER (PARTITION BY map.weather_model, map.grid_cell_id)
        AS ward_count_on_grid,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    CURRENT_TIMESTAMP AS _updated_at
FROM maps AS map
INNER JOIN {{ ref('dim_grid') }} AS grid
    ON grid.grid_cell_id = map.grid_cell_id
