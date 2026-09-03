{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'drought']) }}

WITH rolling AS (
    SELECT
        *,
        {% for days in [30, 60, 90] %}
        SUM(daily_rainfall_mm) OVER (
            PARTITION BY grid_cell_id
            ORDER BY rainfall_date
            RANGE BETWEEN INTERVAL '{{ days - 1 }} days' PRECEDING AND CURRENT ROW
        ) AS rain_{{ days }}d_sum_raw,
        COUNT(daily_rainfall_mm) OVER (
            PARTITION BY grid_cell_id
            ORDER BY rainfall_date
            RANGE BETWEEN INTERVAL '{{ days - 1 }} days' PRECEDING AND CURRENT ROW
        ) AS rain_{{ days }}d_complete_days{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ ref('fct_rainfall_historical_daily') }}
)

SELECT
    historical_daily_key AS drought_daily_key,
    grid_cell_id,
    weather_model,
    weather_product,
    rainfall_date,
    daily_rainfall_mm,
    coverage_ratio AS daily_coverage_ratio,
    is_complete AS daily_is_complete,
    {% for days in [30, 60, 90] %}
    CASE WHEN rain_{{ days }}d_complete_days = {{ days }}
         THEN rain_{{ days }}d_sum_raw END AS rain_{{ days }}d_mm,
    LEAST(rain_{{ days }}d_complete_days / {{ days }}.0, 1.0)
        AS rain_{{ days }}d_coverage_ratio,
    rain_{{ days }}d_complete_days = {{ days }} AS rain_{{ days }}d_is_complete{% if not loop.last %},{% endif %}
    {% endfor %}
FROM rolling
