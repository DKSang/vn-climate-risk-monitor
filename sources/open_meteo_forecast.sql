-- Forecast payload transform used by the autoloader.
-- {{ ingested_at }} comes from the Postgres control-plane clock used by checkpoints.
-- TRY_CAST preserves bad values in _rescued_data instead of failing the batch.

WITH raw AS (
    SELECT *
    FROM read_json_auto(
        {{ files }},
        filename = true,
        union_by_name = true,
        maximum_object_size = 209715200
    )
),

exploded AS (
    SELECT
        filename,
        latitude,
        longitude,
        elevation,
        timezone,
        utc_offset_seconds,
        hourly_units,
        -- Error payloads do not include hourly data.
        UNNEST(hourly.time)                      AS t_raw,
        UNNEST(hourly.precipitation)             AS precipitation_raw,
        UNNEST(hourly.rain)                      AS rain_raw,
        UNNEST(hourly.showers)                   AS showers_raw,
        UNNEST(hourly.precipitation_probability) AS pop_raw,
        UNNEST(hourly.weather_code)              AS weather_code_raw
    FROM raw
    WHERE hourly IS NOT NULL
)

SELECT
    TRY_CAST(latitude  AS DOUBLE)                     AS grid_latitude,
    TRY_CAST(longitude AS DOUBLE)                     AS grid_longitude,
    TRY_CAST(elevation AS DOUBLE)                     AS elevation_m,
    TO_TIMESTAMP(TRY_CAST(CAST(t_raw AS VARCHAR) AS BIGINT)) AS valid_time_utc,
    TRY_CAST(precipitation_raw AS DOUBLE)             AS precipitation_mm,
    TRY_CAST(rain_raw          AS DOUBLE)             AS rain_mm,
    TRY_CAST(showers_raw       AS DOUBLE)             AS showers_mm,
    TRY_CAST(pop_raw           AS INTEGER)            AS precipitation_probability_pct,
    TRY_CAST(weather_code_raw  AS INTEGER)            AS weather_code,
    CAST(timezone AS VARCHAR)                         AS timezone,
    TRY_CAST(utc_offset_seconds AS INTEGER)           AS utc_offset_seconds,
    CAST(hourly_units AS VARCHAR)                     AS hourly_units_json,
    filename                                          AS _source_file,
    {{ ingested_at }}                                 AS _ingested_at,
    -- Keep invalid source values for inspection.
    NULLIF(
        TRIM(
            CASE WHEN TRY_CAST(CAST(t_raw AS VARCHAR) AS BIGINT) IS NULL
                 THEN 'time=' || CAST(t_raw AS VARCHAR) || ' ' ELSE '' END ||
            CASE WHEN precipitation_raw IS NOT NULL
                  AND TRY_CAST(precipitation_raw AS DOUBLE) IS NULL
                 THEN 'precipitation=' || CAST(precipitation_raw AS VARCHAR) || ' ' ELSE '' END ||
            CASE WHEN weather_code_raw IS NOT NULL
                  AND TRY_CAST(weather_code_raw AS INTEGER) IS NULL
                 THEN 'weather_code=' || CAST(weather_code_raw AS VARCHAR) ELSE '' END
        ), ''
    )                                                 AS _rescued_data
FROM exploded
