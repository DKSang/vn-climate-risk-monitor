/* Rain pressure signal by forecast run × ward × hour. */

{{ config(
    materialized = 'table',
    tags = ['fact', 'forecast', 'alert']
) }}

{% set thresholds = var('rain_pressure_thresholds', {}) %}
{% set watch_next_6h = thresholds.get('watch_next_6h_mm', 30) %}
{% set watch_next_24h = thresholds.get('watch_next_24h_mm', 50) %}
{% set elevated_next_6h = thresholds.get('elevated_next_6h_mm', 50) %}
{% set elevated_next_24h = thresholds.get('elevated_next_24h_mm', 100) %}
{% set high_next_1h = thresholds.get('high_next_1h_mm', 70) %}
{% set high_next_3h = thresholds.get('high_next_3h_mm', 50) %}
{% set high_next_6h = thresholds.get('high_next_6h_mm', 100) %}
{% set revision_up = thresholds.get('revision_up_mm', 10) %}

WITH history AS (
    SELECT *
    FROM {{ ref('fct_rain_forecast_hourly') }}
    WHERE weather_model = '{{ var('forecast_model', 'ecmwf_ifs') }}'
),

ranked_runs AS (
    SELECT
        forecast_run_id,
        ROW_NUMBER() OVER (
            ORDER BY {{ forecast_run_order() }}
        ) AS run_rank
    FROM history
    GROUP BY forecast_run_id
),

run_pressure AS (
    SELECT
        f.grid_cell_id,
        f.valid_time_utc,
        MAX(CASE WHEN r.run_rank = 2 THEN f.forecast_next_24h_mm END)
            AS previous_next_24h_mm,
        MAX(CASE WHEN r.run_rank = 2 THEN f.forecast_run_id END)
            AS previous_forecast_run_id,
        COUNT(*) FILTER (
            WHERE r.run_rank <= 3
              AND f.forecast_next_6h_mm >= {{ watch_next_6h }}
        ) AS persistence_runs
    FROM history AS f
    JOIN ranked_runs AS r USING (forecast_run_id)
    WHERE r.run_rank <= 3
    GROUP BY f.grid_cell_id, f.valid_time_utc
),

current_forecast AS (
    SELECT
        f.forecast_run_id,
        f.weather_model,
        f.grid_cell_id,
        f.valid_time_utc,
        f.forecast_run_at AS issued_at_utc,
        b.ward_code,
        f.forecast_next_1h_mm,
        f.forecast_next_3h_mm,
        f.forecast_next_6h_mm,
        f.forecast_next_24h_mm,
        p.previous_forecast_run_id,
        p.previous_next_24h_mm,
        COALESCE(p.persistence_runs, 0) AS persistence_runs
    FROM {{ ref('fct_rain_forecast_current_hourly') }} AS f
    JOIN {{ ref('bridge_ward_grid') }} AS b
        ON b.grid_cell_id = f.grid_cell_id
       AND b.weather_model = f.weather_model
       AND b.is_active = TRUE
    LEFT JOIN run_pressure AS p
        ON p.grid_cell_id = f.grid_cell_id
       AND p.valid_time_utc = f.valid_time_utc
),

features AS (
    SELECT
        *,
        forecast_next_24h_mm - previous_next_24h_mm AS revision_24h_mm,
        CASE
            WHEN forecast_next_1h_mm IS NOT NULL
             AND forecast_next_3h_mm IS NOT NULL
             AND forecast_next_6h_mm IS NOT NULL
             AND forecast_next_24h_mm IS NOT NULL
                THEN 'COMPLETE'
            WHEN forecast_next_1h_mm IS NULL
             AND forecast_next_3h_mm IS NULL
             AND forecast_next_6h_mm IS NULL
             AND forecast_next_24h_mm IS NULL
                THEN 'NONE'
            ELSE 'PARTIAL'
        END AS coverage_status,
        CASE
            WHEN forecast_next_24h_mm IS NULL THEN 'UNKNOWN'
            WHEN previous_next_24h_mm IS NULL THEN 'NEW'
            WHEN forecast_next_24h_mm - previous_next_24h_mm >= {{ revision_up }}
                THEN 'RISING'
            WHEN forecast_next_24h_mm - previous_next_24h_mm <= -{{ revision_up }}
                THEN 'FALLING'
            ELSE 'STABLE'
        END AS revision_direction,
        CASE
            WHEN forecast_next_1h_mm IS NOT NULL
              OR forecast_next_3h_mm IS NOT NULL
              OR forecast_next_6h_mm IS NOT NULL
              OR forecast_next_24h_mm IS NOT NULL
            THEN ROUND(
                LEAST(
                    100.0,
                    GREATEST(
                        COALESCE(forecast_next_1h_mm / NULLIF({{ high_next_1h }}, 0) * 100, 0),
                        COALESCE(forecast_next_3h_mm / NULLIF({{ high_next_3h }}, 0) * 100, 0),
                        COALESCE(forecast_next_6h_mm / NULLIF({{ high_next_6h }}, 0) * 100, 0),
                        COALESCE(forecast_next_24h_mm / NULLIF({{ elevated_next_24h }}, 0) * 100, 0)
                    )
                ),
                2
            )
        END AS pressure_score
    FROM current_forecast
),

classified AS (
    SELECT
        *,
        CASE
            WHEN coverage_status = 'NONE' THEN 'UNKNOWN'
            WHEN forecast_next_1h_mm >= {{ high_next_1h }}
              OR forecast_next_3h_mm >= {{ high_next_3h }}
              OR forecast_next_6h_mm >= {{ high_next_6h }}
                THEN 'HIGH'
            WHEN forecast_next_6h_mm >= {{ elevated_next_6h }}
              OR forecast_next_24h_mm >= {{ elevated_next_24h }}
              OR persistence_runs >= 3
                THEN 'ELEVATED'
            WHEN forecast_next_6h_mm >= {{ watch_next_6h }}
              OR forecast_next_24h_mm >= {{ watch_next_24h }}
              OR persistence_runs >= 2
              OR revision_direction = 'RISING'
                THEN 'WATCH'
            WHEN coverage_status <> 'COMPLETE' THEN 'UNKNOWN'
            ELSE 'NORMAL'
        END AS pressure_level,
        NULLIF(
            CONCAT_WS(
                ', ',
                CASE WHEN forecast_next_1h_mm >= {{ high_next_1h }}
                    THEN 'next_1h_high' END,
                CASE WHEN forecast_next_3h_mm >= {{ high_next_3h }}
                    THEN 'next_3h_high' END,
                CASE WHEN forecast_next_6h_mm >= {{ elevated_next_6h }}
                    THEN 'next_6h_elevated' END,
                CASE WHEN forecast_next_24h_mm >= {{ watch_next_24h }}
                    THEN 'next_24h_watch' END,
                CASE WHEN persistence_runs >= 2
                    THEN 'persistent_runs' END,
                CASE WHEN revision_direction = 'RISING'
                    THEN 'forecast_rising' END,
                CASE WHEN coverage_status <> 'COMPLETE'
                    THEN 'incomplete_forecast_coverage' END
            ),
            ''
        ) AS trigger_reasons
    FROM features
)

SELECT
    MD5(CONCAT_WS(
        '|', forecast_run_id, ward_code, {{ stable_timestamp('valid_time_utc') }}
    ))
        AS rain_pressure_alert_key,
    forecast_run_id,
    weather_model,
    ward_code,
    grid_cell_id,
    valid_time_utc,
    issued_at_utc,
    pressure_level,
    pressure_score,
    coverage_status,
    trigger_reasons,
    forecast_next_1h_mm,
    forecast_next_3h_mm,
    forecast_next_6h_mm,
    forecast_next_24h_mm,
    previous_forecast_run_id,
    previous_next_24h_mm,
    revision_24h_mm,
    revision_direction,
    persistence_runs,
    {{ processing_updated_at() }} AS _updated_at
FROM classified
