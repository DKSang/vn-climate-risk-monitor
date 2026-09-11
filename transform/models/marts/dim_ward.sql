/* Hanoi ward dimension. Incremental giữ state soft-delete. */
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
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('stg_seed__ward') }}
