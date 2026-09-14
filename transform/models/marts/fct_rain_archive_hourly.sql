/* Archive rain fact by grid × hour. NULL window means incomplete coverage. */

{{ config(
    materialized = 'incremental',
    unique_key = 'rain_archive_hourly_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns',
    tags = ['fact', 'rain', 'archive']
) }}

{% set windows = rain_windows() %}
{% set lookback = (windows | max - 1) ~ ' hours' %}

WITH source AS (
    SELECT *
    FROM {{ ref('int_weather_archive_hourly') }}
    {{ incremental_input_scope(
        relation = ref('int_weather_archive_hourly'),
        source_ref = 'stg_weather_archive_hourly',
        dimension = 'valid_time_utc',
        change_column = '_updated_at',
        expand_backward = lookback,
        expand_forward = lookback,
        keys = ['grid_cell_id']
    ) }}
),

windowed AS (
    SELECT
        grid_cell_id,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at,
        _updated_at,
        {{ rolling_rain_sums(windows, partition_by='grid_cell_id') }}
    FROM source
),

published AS (
    SELECT
        MD5(CONCAT_WS('|', grid_cell_id, CAST(valid_time_utc AS VARCHAR)))
            AS rain_archive_hourly_key,
        grid_cell_id,
        valid_time_utc,
        CAST(valid_time_utc AS DATE) AS rain_date,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        {{ rolling_rain_columns(windows) }},
        _source_file,
        _ingested_at,
        _updated_at
    FROM windowed
)

SELECT
    *,
    {{ hanoi_rain_scenario_band('rain_1h_mm') }} AS hanoi_rain_scenario_band,
    {{ hanoi_rain_scenario_level('rain_1h_mm') }} AS hanoi_rain_scenario_level,
    {{ vn_rain_band_12h('rain_12h_mm') }} AS vn_rain_band_12h,
    {{ vn_rain_band_24h('rain_24h_mm') }} AS vn_rain_band_24h
FROM published
{{ incremental_output_scope(
    relation = ref('int_weather_archive_hourly'),
    source_ref = 'stg_weather_archive_hourly',
    dimension = 'valid_time_utc',
    change_column = '_updated_at',
    expand_forward = lookback
) }}
