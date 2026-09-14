/* Hanoi ward reference dimension. Rebuilt from the versioned seed. */
{{ config(
    materialized = 'table',
    tags = ['dim', 'forecast', 'archive']
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
    CURRENT_TIMESTAMP AS _updated_at
FROM {{ ref('stg_seed__ward') }}
