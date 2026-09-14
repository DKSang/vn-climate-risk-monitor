/* Verified flood observations; grain = observation_id. */

{{ config(
    materialized = 'incremental',
    unique_key = 'observation_id',
    incremental_strategy = 'delete+insert',
    tags = ['fact', 'flood', 'archive']
) }}

SELECT
    o.observation_id,
    o.event_id,
    {# location_key ổn định giữa nhiều observation cùng tên địa điểm. #}
    MD5(LOWER(TRIM(o.location_name_raw))) AS location_key,
    o.location_name_raw,
    o.ward_code,
    o.latitude,
    o.longitude,
    o.geocode_match_type,
    o.anchor_type,
    o.geocode_confidence,
    o.coordinate_basis,
    o.needs_manual_validation,
    o.geocode_verified_at_utc,
    o.verification_method,
    o.geocode_review_note,
    o.geocode_verified,
    o.observed_at_utc,
    CAST(o.observed_at_utc AS DATE) AS observed_date_utc,
    o.observed_at_precision,
    o.as_of_utc,
    o.is_flooded,
    o.depth_min_cm,
    o.depth_max_cm,
    o.depth_text_raw,
    o.traffic_status,
    o.traffic_text_raw,
    o.source_grade,
    o.source_publisher,
    o.source_url,
    o.source_visualisation_id,
    o.source_visualisation_version,
    o.source_updated_at_utc,
    {# Replay chỉ dùng nguồn đã xác minh, đúng giờ và có ward mapping. #}
    (
        o.source_grade IN ('A', 'B', 'C')
        AND o.observed_at_precision = 'hour'
        AND o.ward_code IS NOT NULL
    ) AS is_replay_eligible,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('stg_seed__flood_observation') }} o
