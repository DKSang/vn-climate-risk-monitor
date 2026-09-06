/*
    MART — Cầu nối điểm úng ngập ↔ ô lưới thời tiết.

    Grain: (point_id, weather_model).
    Mỗi điểm úng ngập thuộc một phường, và ánh xạ sang ô lưới theo CHÍNH phép snap
    của phường đó (`stg_seed__ward_grid`), đảm bảo tính đồng nhất forcing khí tượng
    với mọi phân tích ở cấp phường.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'flood_point_grid_key',
    tags = ['bridge']
) }}

SELECT
    MD5(CONCAT_WS('|', fp.point_id, wg.weather_model)) AS flood_point_grid_key,
    fp.point_id,
    fp.rain_scenario,
    wg.weather_model,
    wg.grid_cell_id,
    wg.grid_elevation_m,
    fp.is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('dim_flood_point') }} fp
INNER JOIN {{ ref('stg_seed__ward_grid') }} wg
    ON wg.ward_code = fp.ward_code
INNER JOIN {{ ref('dim_grid') }} grid
    ON grid.grid_cell_id = wg.grid_cell_id
WHERE fp.ward_code IS NOT NULL
