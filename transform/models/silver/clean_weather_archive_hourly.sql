/* One row per grid cell x hour of archive weather: staging deduplicated, then merged. */

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['grid_cell_id', 'valid_at'],
    merge_exclude_columns = ['_inserted_at'],
    tags = ['archive']
) }}

WITH new_rows AS (
    SELECT *
    FROM {{ source('silver_staging', 'stg_open_meteo_archive') }}
    {% if is_incremental() and var('watermark', none) %}
    -- Incremental pattern (docs/adr/0001): rows staged since the last successful run.
    WHERE _inserted_at > '{{ var("watermark") }}'::TIMESTAMPTZ
    {% endif %}
),

latest AS (
    -- DuckLake MERGE keeps an arbitrary copy of duplicate keys: dedup first.
    SELECT *
    FROM new_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY grid_latitude, grid_longitude, valid_at
        ORDER BY _inserted_at DESC, _source_file DESC
    ) = 1
)

SELECT
    {{ grid_cell_id("'" ~ var('forecast_model') ~ "'", 'grid_latitude', 'grid_longitude') }}
        AS grid_cell_id,
    valid_at,
    '{{ var("forecast_model") }}' AS weather_model,
    grid_latitude,
    grid_longitude,
    precipitation AS precipitation_mm,
    rain AS rain_mm,
    CAST(weather_code AS INTEGER) AS weather_code,
    _source_file,
    NOW() AS _inserted_at,
    NOW() AS _updated_at
FROM latest
