/* Thin view trên append-only archive staging. Dedup nằm ở intermediate. */

{{ config(materialized = 'view') }}

SELECT
    weather_model,
    grid_latitude,
    grid_longitude,
    elevation_m,
    valid_time_utc,
    precipitation_mm,
    rain_mm,
    weather_code,
    soil_moisture_0_to_7cm,
    soil_moisture_7_to_28cm,
    timezone,
    utc_offset_seconds,
    _source_file,
    _ingested_at
FROM {{ source('silver_staging', 'stg_weather_archive_hourly') }}
WHERE valid_time_utc IS NOT NULL
