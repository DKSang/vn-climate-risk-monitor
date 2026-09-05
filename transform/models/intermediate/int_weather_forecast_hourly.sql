/*
    INTERMEDIATE — một dòng hiện hành cho mỗi (ô lưới forecast, valid_time).
    (Lớp SILVER_CLEAN trong Silver Layer Flow: dedup + upsert change-aware.)

    Giữ dự báo MỚI NHẤT cho mỗi valid_time_utc của từng ô lưới.
    Khi một đợt fetch mới có dự báo cập nhật cho cùng giờ, nó sẽ ghi đè bản cũ
    nếu _row_hash thay đổi.
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
        _source_file,
        _ingested_at
    FROM {{ ref('stg_open_meteo__weather_forecast_hourly') }}
    {{ incremental_changed_filter(
        source_ref = 'stg_weather_forecast',
        change_column = '_ingested_at',
        prefix = 'WHERE'
    ) }}
),

-- Grain là (model, ô, valid_time_utc). Lần ingest MỚI NHẤT thắng (latest forecast).
deduplicated AS (
    SELECT *
    FROM staged
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
