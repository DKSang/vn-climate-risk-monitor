/* One row per forecast run x grid cell x hour: staging deduplicated, then merged. */

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['forecast_run', 'grid_cell_id', 'valid_at'],
    merge_exclude_columns = ['_inserted_at'],
    tags = ['forecast']
) }}

WITH new_rows AS (
    SELECT *
    FROM {{ source('silver_staging', 'stg_open_meteo_forecast') }}
    {% if is_incremental() and var('watermark', none) %}
    -- Incremental pattern (docs/adr/0001): rows staged since the last successful run.
    WHERE _inserted_at > '{{ var("watermark") }}'::TIMESTAMPTZ
    {% endif %}
),

latest AS (
    -- DuckLake MERGE accepts duplicate source keys and keeps an arbitrary one,
    -- so keep the most recently staged copy of each key first.
    SELECT *
    FROM new_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY forecast_run, grid_latitude, grid_longitude, valid_at
        ORDER BY _inserted_at DESC, _source_file DESC
    ) = 1
)

SELECT
    forecast_run,
    {{ grid_cell_id("'" ~ var('forecast_model') ~ "'", 'grid_latitude', 'grid_longitude') }}
        AS grid_cell_id,
    valid_at,
    '{{ var("forecast_model") }}' AS weather_model,
    grid_latitude,
    grid_longitude,
    precipitation AS precipitation_mm,
    rain AS rain_mm,
    showers AS showers_mm,
    CAST(precipitation_probability AS INTEGER) AS precipitation_probability_pct,
    CAST(weather_code AS INTEGER) AS weather_code,
    _source_file,
    NOW() AS _inserted_at,
    NOW() AS _updated_at
FROM latest
