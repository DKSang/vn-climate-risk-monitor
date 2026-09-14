/* Flood-point reference dimension. Rebuilt from the versioned seed. */

{{ config(
    materialized = 'table',
    tags = ['dim', 'forecast', 'archive']
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
    CASE rain_scenario
        WHEN 'scenario_50_70mm' THEN 1
        WHEN 'scenario_70_100mm' THEN 2
        WHEN 'scenario_over_100mm' THEN 3
    END AS required_rain_scenario_level,
    typical_depth_cm,
    latitude,
    longitude,
    status,
    source_reference,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    CURRENT_TIMESTAMP AS _updated_at
FROM {{ ref('stg_seed__flood_point') }}
