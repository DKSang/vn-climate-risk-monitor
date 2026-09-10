/*
    MART HISTORY — mưa dự báo theo vintage × ô lưới × giờ.
    `rain_*h_mm` là trailing context; `forecast_next_*h_mm` là lượng mưa
    forecast strictly sau valid time hiện tại (t+1h … t+Hh) để phục vụ pressure
    alert. Row tại valid time đã thuộc giờ vừa kết thúc.
    Rolling partition theo forecast_run_id để không trộn hai vintage.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'rain_forecast_hourly_key',
    tags = ['fact', 'rain', 'forecast']
) }}

{% set windows = rain_windows() %}
{% set lookback = (windows | max - 1) ~ ' hours' %}

WITH source AS (
    SELECT *
    FROM {{ ref('int_weather_forecast_hourly') }}
    {{ incremental_input_scope(
        relation = ref('int_weather_forecast_hourly'),
        source_ref = 'int_weather_forecast_hourly',
        dimension = 'valid_time_utc',
        change_column = '_updated_at',
        expand_backward = lookback,
        expand_forward = lookback,
        keys = ['forecast_run_id', 'grid_cell_id']
    ) }}
),

windowed AS (
    SELECT
        grid_cell_id,
        forecast_run_id,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        showers_mm,
        precipitation_probability_pct,
        weather_code,
        _source_file,
        _ingested_at,
        _updated_at,
        {{ rolling_rain_sums(
            windows,
            partition_by='forecast_run_id, grid_cell_id'
        ) }},
        {{ forward_rain_sums(
            windows,
            partition_by='forecast_run_id, grid_cell_id'
        ) }}
    FROM source
),

published AS (
    SELECT
        MD5(CONCAT_WS(
            '|', forecast_run_id, grid_cell_id, CAST(valid_time_utc AS VARCHAR)
        ))
            AS rain_forecast_hourly_key,
        forecast_run_id,
        grid_cell_id,
        valid_time_utc,
        CAST(valid_time_utc AS DATE) AS forecast_date,
        precipitation_mm,
        rain_mm,
        showers_mm,
        precipitation_probability_pct,
        weather_code,
        {{ rolling_rain_columns(windows) }},
        {{ forward_rain_columns(windows) }},
        _source_file,
        _ingested_at,
        _updated_at
    FROM windowed
)

SELECT
    *,
    {{ hanoi_rain_scenario_band('rain_1h_mm') }} AS hanoi_rain_scenario_band,
    {{ vn_rain_band_12h('rain_12h_mm') }} AS vn_rain_band_12h,
    {{ vn_rain_band_24h('rain_24h_mm') }} AS vn_rain_band_24h
FROM published
{{ incremental_output_scope(
    relation = ref('int_weather_forecast_hourly'),
    source_ref = 'int_weather_forecast_hourly',
    dimension = 'valid_time_utc',
    change_column = '_updated_at',
    expand_backward = lookback,
    expand_forward = lookback
) }}
