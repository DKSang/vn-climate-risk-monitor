/* Flood observations plus reviewed geocode metadata. */

{{ config(materialized = 'view') }}

WITH seed_data AS (
    SELECT
        observation_id,
        event_id,
        location_name_raw,
        depth_text_raw,
        traffic_text_raw,
        depth_min_cm,
        depth_max_cm,
        traffic_status,
        NULLIF(TRIM(observed_at_local), '') AS observed_at_local,
        NULLIF(TRIM(as_of_local), '') AS as_of_local,
        is_flooded,
        source_grade,
        source_publisher,
        source_url,
        source_visualisation_id,
        source_visualisation_version,
        source_updated_at_utc
    FROM {{ ref('flood_event_observations_seed') }}
),

geocode AS (
    SELECT
        observation_id,
        latitude,
        longitude,
        ward_code,
        geocode_match_type,
        geocode_verified,
        anchor_type,
        confidence AS geocode_confidence,
        coordinate_basis,
        needs_manual_validation,
        NULLIF(TRIM(verified_at_utc), '') AS verified_at_utc,
        NULLIF(TRIM(verification_method), '') AS verification_method,
        review_note
    FROM {{ ref('flood_observation_geocode_seed') }}
)

SELECT
    s.observation_id,
    event_id,
    location_name_raw,
    {# Seed đã có +07:00; CAST trực tiếp tránh apply timezone hai lần. #}
    CAST(observed_at_local AS TIMESTAMPTZ) AS observed_at_utc,
    observed_at_local,
    CASE WHEN observed_at_local IS NULL THEN 'unknown' ELSE 'hour' END
        AS observed_at_precision,
    CAST(as_of_local AS TIMESTAMPTZ) AS as_of_utc,
    is_flooded,
    depth_min_cm,
    depth_max_cm,
    depth_text_raw,
    traffic_status,
    traffic_text_raw,
    source_grade,
    source_publisher,
    source_url,
    source_visualisation_id,
    source_visualisation_version,
    CAST(source_updated_at_utc AS TIMESTAMPTZ) AS source_updated_at_utc,
    g.latitude,
    g.longitude,
    g.geocode_match_type,
    g.anchor_type,
    g.geocode_confidence,
    g.coordinate_basis,
    COALESCE(g.needs_manual_validation, TRUE) AS needs_manual_validation,
    CAST(g.verified_at_utc AS TIMESTAMPTZ) AS geocode_verified_at_utc,
    g.verification_method,
    g.review_note AS geocode_review_note,
    COALESCE(g.geocode_verified, FALSE) AS geocode_verified,
    -- ward_code chỉ publish sau khi geocode được review.
    CASE WHEN g.geocode_verified THEN g.ward_code END AS ward_code
FROM seed_data s
LEFT JOIN geocode g USING (observation_id)
