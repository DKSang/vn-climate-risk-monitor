/* Hanoi ward reference dimension. Rebuilt from the versioned seed. */
{{ config(
    materialized = 'table',
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
    climate_zone
FROM {{ ref('stg_seed__ward') }}
