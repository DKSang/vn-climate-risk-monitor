/*
    GOLD — 126 phường/xã Hà Nội.

    KHÔNG mang `grid_cell_id`: một phường ánh xạ sang ô KHÁC NHAU tuỳ model
    (era5 12 ô, ecmwf_ifs 48 ô). Quan hệ đó là many-to-many theo model nên nó
    thuộc về `bridge_ward_grid`, không thuộc về dimension.
*/

{{ config(
    materialized = 'table',
    tags = ['gold', 'dim']
) }}

SELECT
    ward_code,
    ward_name,
    province_code,
    province_name,
    ward_latitude,
    ward_longitude,
    region,
    climate_zone
FROM {{ ref('ward') }}
