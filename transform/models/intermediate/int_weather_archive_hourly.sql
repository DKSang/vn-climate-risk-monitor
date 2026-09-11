/* Curated archive grain: model × grid × hour, dedup + change-aware merge. */

{{ config(
    materialized = 'incremental',
    unique_key = 'weather_archive_hourly_key',
    tags = ['intermediate']
) }}

{# elevation_m không ổn định theo requested point nên không thuộc row hash. #}
{% set value_columns = [
    'precipitation_mm',
    'rain_mm',
    'weather_code',
    'soil_moisture_0_to_7cm',
    'soil_moisture_7_to_28cm',
] %}

WITH staged AS (
    SELECT
        weather_model,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at
    FROM {{ ref('stg_open_meteo__weather_archive_hourly') }}
    {{ incremental_changed_filter(
        source_ref = 'stg_weather_archive_hourly',
        change_column = '_ingested_at',
        prefix = 'WHERE'
    ) }}
),

deduplicated AS (
    SELECT *
    FROM staged
    -- Value columns là tie-break cuối để dedup deterministic.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY weather_model, grid_latitude, grid_longitude, valid_time_utc
        ORDER BY
            _ingested_at DESC,
            _source_file DESC
            {%- for column in value_columns %},
            {{ column }}
            {%- endfor %}
    ) = 1
),

incoming AS (
    SELECT
        MD5(CONCAT_WS(
            '|',
            {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }},
            CAST(valid_time_utc AS VARCHAR)
        )) AS weather_archive_hourly_key,
        {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }}
            AS grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        {% for column in value_columns %}{{ column }},
        {% endfor %}
        {{ stable_row_hash(value_columns) }} AS _row_hash,
        _source_file,
        _ingested_at
    FROM deduplicated
)

SELECT
    incoming.*,
    TRUE AS is_active,
    {{ processing_updated_at() }} AS _updated_at
FROM incoming

{{ incremental_new_or_changed('weather_archive_hourly_key') }}
