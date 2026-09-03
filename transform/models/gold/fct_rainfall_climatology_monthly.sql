{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'climatology']) }}

/*
    Empirical monthly baseline over each model's available archive.
    Climate window: all_available_by_model_v1 — not WMO 1991–2020.
*/

WITH rolling_samples AS (
    {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
    SELECT
        grid_cell_id,
        weather_model,
        weather_product,
        calendar_month,
        valid_time_utc,
        {{ hours }} AS window_hours,
        rain_{{ hours }}h_mm AS rainfall_mm
    FROM {{ ref('fct_rainfall_historical_hourly') }}
    WHERE rain_{{ hours }}h_is_complete
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

aggregated AS (
    SELECT
        grid_cell_id,
        weather_model,
        weather_product,
        calendar_month,
        window_hours,
        COUNT(*) AS observation_count,
        MIN(valid_time_utc) AS baseline_first_observation_utc,
        MAX(valid_time_utc) AS baseline_last_observation_utc,
        AVG(rainfall_mm) AS mean_rainfall_mm,
        MEDIAN(rainfall_mm) AS median_rainfall_mm,
        QUANTILE_CONT(rainfall_mm, 0.95) AS p95_rainfall_mm,
        QUANTILE_CONT(rainfall_mm, 0.99) AS p99_rainfall_mm
    FROM rolling_samples
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    MD5(CONCAT_WS(
        '|', grid_cell_id, CAST(calendar_month AS VARCHAR),
        CAST(window_hours AS VARCHAR), 'all_available_by_model_v1'
    )) AS climatology_key,
    grid_cell_id,
    weather_model,
    weather_product,
    calendar_month,
    window_hours,
    'all_available_by_model_v1' AS climatology_window_id,
    CAST(DATE_PART('year', baseline_first_observation_utc) AS INTEGER)
        AS climatology_start_year,
    CAST(DATE_PART('year', baseline_last_observation_utc) AS INTEGER)
        AS climatology_end_year,
    observation_count,
    baseline_first_observation_utc,
    baseline_last_observation_utc,
    mean_rainfall_mm,
    median_rainfall_mm,
    p95_rainfall_mm,
    p99_rainfall_mm,
    observation_count >= 100 AS is_sample_sufficient
FROM aggregated
