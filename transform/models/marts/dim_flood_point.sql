/*
    MART — Dimension điểm úng ngập Hà Nội.

    Incremental để bảo toàn lịch sử và cờ soft-delete (`is_active = FALSE`)
    nếu điểm ngập được giải tỏa/sửa chữa và cập nhật trong tương lai.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'point_id',
    tags = ['dim']
) }}

SELECT
    point_id,
    point_name,
    street_name,
    ward_name,
    ward_code,
    district_name,
    drainage_basin,
    rain_scenario,
    typical_depth_cm,
    latitude,
    longitude,
    status,
    source_reference,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('stg_seed__flood_point') }}
