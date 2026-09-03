{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'drought', 'hanoi']) }}

SELECT
    MD5(CONCAT_WS(
        '|', CAST(mapping.ward_key AS VARCHAR), drought.grid_cell_id,
        CAST(drought.rainfall_date AS VARCHAR)
    )) AS ward_drought_daily_key,
    mapping.ward_key,
    mapping.ward_code,
    mapping.ward_name,
    mapping.requested_latitude,
    mapping.requested_longitude,
    mapping.mapping_distance_km,
    drought.*
FROM {{ ref('ward_grid_map') }} AS mapping
INNER JOIN {{ ref('fct_rainfall_drought_daily') }} AS drought
    ON mapping.grid_cell_id = drought.grid_cell_id
