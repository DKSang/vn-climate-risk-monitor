{{ config(
    materialized = 'incremental',
    unique_key = 'historical_hourly_key',
    tags = ['gold', 'kpi', 'rainfall', 'historical']
) }}

{#
    Cửa sổ rolling rộng nhất của model này là 72h, nên một row mới ở giờ T làm
    sai các row đầu ra trong [T, T+71h] và để tính chúng phải đọc từ T−71h.
    Khai báo MỘT LẦN ở đây; hai macro dưới cùng đọc chung biến này.
#}
{% set lookback = '71 hours' %}

WITH source AS (
    SELECT *
    FROM {{ ref('archive_hourly') }}
    {{ incremental_input_scope(
        relation = ref('archive_hourly'),
        source_ref = 'archive_hourly',
        dimension = 'valid_time_utc',
        expand_backward = lookback,
        expand_forward = lookback,
        keys = ['grid_cell_id']
    ) }}
),

windowed AS (
    SELECT
        grid_cell_id,
        weather_model,
        weather_product,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at,
        {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
        SUM(precipitation_mm) OVER (
            PARTITION BY grid_cell_id
            ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
        ) AS rain_{{ hours }}h_sum_raw,
        COUNT(precipitation_mm) OVER (
            PARTITION BY grid_cell_id
            ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
        ) AS rain_{{ hours }}h_observed_hours{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM source
)

SELECT
    MD5(CONCAT_WS('|', grid_cell_id, CAST(valid_time_utc AS VARCHAR)))
        AS historical_hourly_key,
    grid_cell_id,
    weather_model,
    weather_product,
    valid_time_utc,
    CAST(DATE_PART('month', valid_time_utc AT TIME ZONE 'UTC') AS INTEGER) AS calendar_month,
    precipitation_mm,
    rain_mm,
    weather_code,
    soil_moisture_0_to_7cm,
    soil_moisture_7_to_28cm,
    {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
    CASE WHEN rain_{{ hours }}h_observed_hours = {{ hours }}
         THEN rain_{{ hours }}h_sum_raw END AS rain_{{ hours }}h_mm,
    LEAST(rain_{{ hours }}h_observed_hours / {{ hours }}.0, 1.0)
        AS rain_{{ hours }}h_coverage_ratio,
    rain_{{ hours }}h_observed_hours = {{ hours }} AS rain_{{ hours }}h_is_complete,
    {% endfor %}
    CASE
        WHEN rain_1h_observed_hours <> 1 THEN NULL
        WHEN rain_1h_sum_raw > 100 THEN 'over_100'
        WHEN rain_1h_sum_raw >= 70 THEN 'from_70_to_100'
        WHEN rain_1h_sum_raw >= 50 THEN 'from_50_to_under_70'
        WHEN rain_1h_sum_raw >= 0 THEN 'below_50'
    END AS hanoi_rain_scenario_band,
    CASE
        WHEN rain_12h_observed_hours <> 12 THEN NULL
        WHEN rain_12h_sum_raw > 100 THEN 'over_100'
        WHEN rain_12h_sum_raw >= 50 THEN 'from_50_to_100'
        WHEN rain_12h_sum_raw >= 0 THEN 'below_50'
    END AS vn_rain_band_12h,
    CASE
        WHEN rain_24h_observed_hours <> 24 THEN NULL
        WHEN rain_24h_sum_raw > 400 THEN 'over_400'
        WHEN rain_24h_sum_raw > 200 THEN 'over_200_to_400'
        WHEN rain_24h_sum_raw >= 100 THEN 'from_100_to_200'
        WHEN rain_24h_sum_raw >= 0 THEN 'below_100'
    END AS vn_rain_band_24h,
    CASE
        WHEN rain_12h_observed_hours = 12 AND rain_12h_sum_raw >= 50 THEN TRUE
        WHEN rain_24h_observed_hours = 24 AND rain_24h_sum_raw >= 100 THEN TRUE
        WHEN rain_12h_observed_hours = 12 AND rain_24h_observed_hours = 24 THEN FALSE
    END AS vn_rain_threshold_exceeded,
    _source_file,
    _ingested_at
FROM windowed
{{ incremental_output_scope(
    relation = ref('archive_hourly'),
    source_ref = 'archive_hourly',
    dimension = 'valid_time_utc',
    expand_forward = lookback
) }}
