/* Forecast vintage history; chỉ publish run đủ location × horizon. */

{{ config(
    materialized = 'incremental',
    unique_key = 'weather_forecast_hourly_key',
    incremental_strategy = 'delete+insert',
    tags = ['intermediate', 'forecast']
) }}

{% set value_columns = [
    'precipitation_mm',
    'rain_mm',
    'showers_mm',
    'precipitation_probability_pct',
    'weather_code',
] %}

WITH raw_rows AS (
    SELECT
        'ecmwf_ifs_fc' AS weather_model,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        showers_mm,
        precipitation_probability_pct,
        weather_code,
        REGEXP_EXTRACT(
            _source_file,
            '/incremental/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/(run_[0-9]{8}T[0-9]{6})/',
            1
        ) AS forecast_run_id,
        _source_file,
        _ingested_at
    FROM {{ source('silver_staging', 'stg_weather_forecast') }}
),

changed_rows AS (
    SELECT *
    FROM raw_rows
    {{ incremental_changed_filter(
        source_ref = 'stg_weather_forecast',
        change_column = '_ingested_at',
        prefix = 'WHERE'
    ) }}
),

changed_runs AS (
    SELECT DISTINCT forecast_run_id
    FROM changed_rows
    WHERE forecast_run_id <> ''
),

staged AS (
    SELECT *
    FROM raw_rows
    WHERE forecast_run_id IN (SELECT forecast_run_id FROM changed_runs)
),

normalized AS (
    SELECT
        staged.*,
        {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }}
            AS grid_cell_id
    FROM staged
),

run_hours AS (
    SELECT
        forecast_run_id,
        valid_time_utc,
        COUNT(*) AS locations
    FROM normalized
    WHERE forecast_run_id <> ''
    GROUP BY forecast_run_id, valid_time_utc
),

complete_runs AS (
    SELECT forecast_run_id
    FROM run_hours
    GROUP BY forecast_run_id
    HAVING COUNT(DISTINCT valid_time_utc) = {{ var('forecast_expected_hours', 72) }}
       AND COUNT(*) = {{ var('forecast_expected_hours', 72) }}
       AND MIN(locations) = {{ var('forecast_expected_locations', 126) }}
       AND MAX(locations) = {{ var('forecast_expected_locations', 126) }}
),

deduplicated AS (
    SELECT *
    FROM normalized
    WHERE forecast_run_id IN (SELECT forecast_run_id FROM complete_runs)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            forecast_run_id,
            weather_model,
            grid_latitude,
            grid_longitude,
            valid_time_utc
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
            forecast_run_id,
            grid_cell_id,
            {{ stable_timestamp('valid_time_utc') }}
        )) AS weather_forecast_hourly_key,
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        {% for column in value_columns %}{{ column }},
        {% endfor %}
        {{ stable_row_hash(value_columns) }} AS _row_hash,
        forecast_run_id,
        _source_file,
        _ingested_at
    FROM deduplicated
)

SELECT
    incoming.*,
    TRUE AS is_active,
    {{ processing_updated_at() }} AS _updated_at
FROM incoming
