/* Hanoi wards from the canonical seed. */

{{ config(materialized = 'view') }}

SELECT
    commune_code AS ward_code,
    commune_name AS ward_name,
    province_code,
    province_name,
    latitude AS ward_latitude,
    longitude AS ward_longitude,
    region,
    climate_zone,
    TRUE AS is_active
FROM {{ ref('ward_coordinates_seed') }}
WHERE province_code = '01'
