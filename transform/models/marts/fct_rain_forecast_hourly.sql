/* Rainfall per forecast run x grid cell x hour, with rolling and forward windows. */

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['forecast_run', 'grid_cell_id', 'valid_at'],
    merge_exclude_columns = ['_inserted_at'],
    tags = ['forecast']
) }}

{% set windows = rain_windows() %}

WITH runs AS (
    -- Incremental pattern (docs/adr/0001). The unit of recompute is a whole
    -- forecast run: its rain windows span all of its hours.
    SELECT *
    FROM {{ ref('clean_weather_forecast_hourly') }}
    {% if is_incremental() and var('watermark', none) %}
    WHERE forecast_run IN (
        SELECT forecast_run
        FROM {{ ref('clean_weather_forecast_hourly') }}
        WHERE _updated_at > '{{ var("watermark") }}'::TIMESTAMPTZ
    )
    {% endif %}
),

windowed AS (
    SELECT
        *,
        {{ rolling_rain_sums(windows, partition_by='forecast_run, grid_cell_id') }},
        {{ forward_rain_sums(windows, partition_by='forecast_run, grid_cell_id') }}
    FROM runs
),

sums AS (
    SELECT
        forecast_run,
        grid_cell_id,
        valid_at,
        weather_model,
        CAST(valid_at AS DATE) AS forecast_date,
        precipitation_mm,
        rain_mm,
        showers_mm,
        precipitation_probability_pct,
        weather_code,
        {{ rolling_rain_columns(windows) }},
        {{ forward_rain_columns(windows) }}
    FROM windowed
)

SELECT
    *,
    {{ hanoi_rain_scenario_band('rain_1h_mm') }} AS hanoi_rain_scenario_band,
    {{ hanoi_rain_scenario_level('rain_1h_mm') }} AS hanoi_rain_scenario_level,
    {{ vn_rain_band_12h('rain_12h_mm') }} AS vn_rain_band_12h,
    {{ vn_rain_band_24h('rain_24h_mm') }} AS vn_rain_band_24h,
    {{ hanoi_rain_scenario_band('forecast_next_1h_mm') }} AS forecast_next_1h_band,
    {{ vn_rain_band_12h('forecast_next_12h_mm') }} AS forecast_next_12h_band,
    {{ vn_rain_band_24h('forecast_next_24h_mm') }} AS forecast_next_24h_band,
    NOW() AS _inserted_at,
    NOW() AS _updated_at
FROM sums
