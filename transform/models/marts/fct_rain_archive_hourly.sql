/* Past rainfall per grid cell x hour, with rolling windows over the whole history.

Rebuilt in full: windows cross month boundaries, and a year is only ~420k rows.
*/

{{ config(materialized = 'table', tags = ['archive']) }}

{% set windows = rain_windows() %}

WITH windowed AS (
    SELECT
        *,
        {{ rolling_rain_sums(windows, partition_by='grid_cell_id') }}
    FROM {{ ref('clean_weather_archive_hourly') }}
),

sums AS (
    SELECT
        grid_cell_id,
        valid_at,
        weather_model,
        CAST(valid_at AS DATE) AS rain_date,
        precipitation_mm,
        rain_mm,
        weather_code,
        {{ rolling_rain_columns(windows) }}
    FROM windowed
)

SELECT
    *,
    {{ hanoi_rain_scenario_band('rain_1h_mm') }} AS hanoi_rain_scenario_band,
    {{ vn_rain_band_12h('rain_12h_mm') }} AS vn_rain_band_12h,
    {{ vn_rain_band_24h('rain_24h_mm') }} AS vn_rain_band_24h,
    NOW() AS _updated_at
FROM sums
