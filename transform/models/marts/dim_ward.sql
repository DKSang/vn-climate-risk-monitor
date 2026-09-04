/*
    MART — 126 phường/xã Hà Nội.

    KHÔNG mang `grid_cell_id`: một phường ánh xạ sang ô KHÁC NHAU tuỳ model
    (era5 12 ô, ecmwf_ifs 48 ô). Quan hệ đó là many-to-many theo model nên nó
    thuộc về `bridge_ward_grid`, không thuộc về dimension.
*/

{#
    INCREMENTAL chứ không phải `table`: soft delete ghi `is_active = FALSE` vào
    chính bảng này, và `CREATE OR REPLACE` sẽ xoá sạch cờ đó mỗi lần build.
    Materialization incremental chỉ DELETE các key CÓ trong slice, nên phường đã
    giải thể (không còn trong seed → không vào slice) được giữ lại cùng cờ của nó.
#}
{{ config(
    materialized = 'incremental',
    unique_key = 'ward_code',
    tags = ['dim']
) }}

SELECT
    ward_code,
    ward_name,
    province_code,
    province_name,
    ward_latitude,
    ward_longitude,
    region,
    climate_zone,
    -- Phường có trong seed thì đang tồn tại. Cờ chỉ chuyển sang FALSE bởi
    -- soft-delete adapter, chạy sau dbt trong cùng một run.
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('stg_seed__ward') }}
