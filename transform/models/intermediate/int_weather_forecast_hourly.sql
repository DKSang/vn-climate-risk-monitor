-- depends_on: {{ ref('stg_open_meteo__weather_forecast_hourly') }}
/*
    INTERMEDIATE — lịch sử forecast vintage theo
    (forecast_run_id, ô lưới forecast, valid_time).

    Mỗi logical run được giữ để audit revision và persistence.
    Chỉ run đủ 126 location × forecast_hours mới được publish.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'weather_forecast_hourly_key',
    tags = ['intermediate']
) }}

{#
    Cột GIÁ TRỊ dùng cho `_row_hash`. Khác archive: có showers và pop, không có soil_moisture.
#}
{% set value_columns = [
    'precipitation_mm',
    'rain_mm',
    'showers_mm',
    'precipitation_probability_pct',
    'weather_code',
] %}

WITH staged AS (
    SELECT
        weather_model,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        showers_mm,
        precipitation_probability_pct,
        weather_code,
        forecast_run_id,
        _source_file,
        _ingested_at
    FROM {{ ref('stg_open_meteo__weather_forecast_hourly') }}
    {{ incremental_changed_filter(
        source_ref = 'stg_weather_forecast',
        change_column = '_ingested_at',
        prefix = 'WHERE'
    ) }}
),

run_hours AS (
    SELECT
        forecast_run_id,
        valid_time_utc,
        COUNT(*) AS locations
    FROM staged
    WHERE forecast_run_id <> ''
    GROUP BY forecast_run_id, valid_time_utc
),

complete_runs AS (
    SELECT forecast_run_id
    FROM run_hours
    GROUP BY forecast_run_id
    HAVING COUNT(*) = {{ var('forecast_expected_hours', 72) }}
       AND MIN(locations) = {{ var('forecast_expected_locations', 126) }}
       AND MAX(locations) = {{ var('forecast_expected_locations', 126) }}
),

-- Các requested point có thể trùng returned grid; dedup trong TỪNG run.
deduplicated AS (
    SELECT *
    FROM staged
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
            {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }},
            CAST(valid_time_utc AS VARCHAR)
        )) AS weather_forecast_hourly_key,
        {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }}
            AS grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        {% for column in value_columns %}{{ column }},
        {% endfor %}
        MD5(CONCAT_WS('|',
            {%- for column in value_columns %}
            COALESCE(CAST({{ column }} AS VARCHAR), ''){{ "," if not loop.last }}
            {%- endfor %}
        )) AS _row_hash,
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

{% if is_incremental() %}
-- Change-aware MERGE: chỉ giữ dòng MỚI hoặc ĐỔI THẬT.
LEFT JOIN {{ this }} AS existing
    ON existing.weather_forecast_hourly_key = incoming.weather_forecast_hourly_key
WHERE existing.weather_forecast_hourly_key IS NULL
   OR existing._row_hash <> incoming._row_hash
{% endif %}
