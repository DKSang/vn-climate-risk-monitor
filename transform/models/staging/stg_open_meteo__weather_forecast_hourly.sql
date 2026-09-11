/* Thin view trên forecast staging; dedup/canonicalization nằm ở intermediate. */

{{ config(materialized = 'view') }}

SELECT
    'ecmwf_ifs_fc'                                     AS weather_model,
    grid_latitude,
    grid_longitude,
    elevation_m,
    valid_time_utc,
    precipitation_mm,
    rain_mm,
    showers_mm,
    precipitation_probability_pct,
    weather_code,
    timezone,
    utc_offset_seconds,
    REGEXP_EXTRACT(
        _source_file,
        '/incremental/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/(run_[0-9]{8}T[0-9]{6})/',
        1
    )                                                   AS forecast_run_id,
    _source_file,
    _ingested_at
FROM {{ source('silver_staging', 'stg_weather_forecast') }}
WHERE valid_time_utc IS NOT NULL
