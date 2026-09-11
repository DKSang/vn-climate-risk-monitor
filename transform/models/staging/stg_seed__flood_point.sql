/* Flood-point seed normalized to ward_code and 6-decimal coordinates. */

{{ config(materialized = 'view') }}

WITH seed_data AS (
    SELECT
        point_id,
        point_name,
        street_name,
        ward_name,
        district_name,
        drainage_basin,
        rain_scenario,
        typical_depth_cm,
        ROUND(latitude, 6) AS latitude,
        ROUND(longitude, 6) AS longitude,
        status,
        source_reference
    FROM {{ ref('hanoi_flood_points_seed') }}
),

ward_ref AS (
    SELECT
        ward_code,
        ward_name,
        REPLACE(LOWER(ward_name), 'phường ', '') AS clean_ward_name
    FROM {{ ref('stg_seed__ward') }}
)

SELECT
    s.point_id,
    s.point_name,
    s.street_name,
    s.ward_name,
    w.ward_code,
    s.district_name,
    s.drainage_basin,
    s.rain_scenario,
    s.typical_depth_cm,
    s.latitude,
    s.longitude,
    s.status,
    s.source_reference
FROM seed_data s
LEFT JOIN ward_ref w
    ON REPLACE(LOWER(s.ward_name), 'phường ', '') = w.clean_ward_name
    OR LOWER(w.ward_name) LIKE '%' || LOWER(s.ward_name) || '%'
