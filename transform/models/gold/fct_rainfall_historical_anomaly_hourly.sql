{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'climatology']) }}

SELECT
    historical.historical_hourly_key,
    historical.grid_cell_id,
    historical.weather_model,
    historical.weather_product,
    historical.valid_time_utc,
    historical.calendar_month,
    baseline_1h.climatology_window_id,
    {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
    historical.rain_{{ hours }}h_mm,
    historical.rain_{{ hours }}h_coverage_ratio,
    historical.rain_{{ hours }}h_is_complete,
    baseline_{{ hours }}h.observation_count AS rain_{{ hours }}h_baseline_observation_count,
    COALESCE(baseline_{{ hours }}h.is_sample_sufficient, FALSE)
        AS rain_{{ hours }}h_baseline_is_sufficient,
    CASE WHEN baseline_{{ hours }}h.is_sample_sufficient
         THEN baseline_{{ hours }}h.median_rainfall_mm END
        AS rain_{{ hours }}h_monthly_median_mm,
    CASE WHEN baseline_{{ hours }}h.is_sample_sufficient
         THEN historical.rain_{{ hours }}h_mm - baseline_{{ hours }}h.median_rainfall_mm END
        AS rain_{{ hours }}h_anomaly_from_monthly_median_mm,
    CASE WHEN baseline_{{ hours }}h.is_sample_sufficient
         THEN baseline_{{ hours }}h.p95_rainfall_mm END AS rain_{{ hours }}h_p95_mm,
    CASE WHEN baseline_{{ hours }}h.is_sample_sufficient
         THEN baseline_{{ hours }}h.p99_rainfall_mm END AS rain_{{ hours }}h_p99_mm,
    CASE WHEN historical.rain_{{ hours }}h_is_complete
                   AND baseline_{{ hours }}h.is_sample_sufficient
         THEN historical.rain_{{ hours }}h_mm > baseline_{{ hours }}h.p95_rainfall_mm END
        AS rain_{{ hours }}h_exceeds_p95,
    CASE WHEN historical.rain_{{ hours }}h_is_complete
                   AND baseline_{{ hours }}h.is_sample_sufficient
         THEN historical.rain_{{ hours }}h_mm > baseline_{{ hours }}h.p99_rainfall_mm END
        AS rain_{{ hours }}h_exceeds_p99{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('fct_rainfall_historical_hourly') }} AS historical
{% for hours in [1, 3, 6, 12, 24, 48, 72] %}
LEFT JOIN {{ ref('fct_rainfall_climatology_monthly') }} AS baseline_{{ hours }}h
    ON historical.grid_cell_id = baseline_{{ hours }}h.grid_cell_id
   AND historical.calendar_month = baseline_{{ hours }}h.calendar_month
   AND baseline_{{ hours }}h.window_hours = {{ hours }}
   AND baseline_{{ hours }}h.climatology_window_id = 'all_available_by_model_v1'
{% endfor %}
