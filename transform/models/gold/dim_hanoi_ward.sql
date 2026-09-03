{{ config(
    materialized = 'table',
    tags = ['gold', 'dimension', 'hanoi']
) }}

/*
    GOLD — 126 phường/xã Hà Nội. SCD2 từ 2025-07-01 (NQ 1656) → tương lai.
    Không reconstruct ranh giới trước 2025.
*/

SELECT
    location_key            AS ward_key,
    ward_code,
    ward_name,
    COALESCE(ward_name_en, '')   AS ward_name_en,
    COALESCE(ward_slug, '')      AS ward_slug,
    province_code,
    province_name,
    latitude,
    longitude,
    region,
    climate_zone,
    TIMESTAMPTZ '2025-07-01 00:00:00+00' AS valid_from_utc,
    CAST(NULL AS TIMESTAMPTZ) AS valid_to_utc,
    TRUE AS is_current,
    bronze_ingested_at
FROM {{ ref('ward_locations') }}
WHERE province_code = '01'
