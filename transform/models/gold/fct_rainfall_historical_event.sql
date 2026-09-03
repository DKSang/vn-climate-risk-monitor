{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'event']) }}

/*
    Rainfall events use wet > 0.1 mm and start after at least six consecutive,
    observed dry hours. Missing/null hours split observed runs rather than being
    interpreted as dry. Trace rainfall inside an event span remains in its total.
*/

WITH lagged AS (
    SELECT
        grid_cell_id,
        weather_model,
        weather_product,
        valid_time_utc,
        precipitation_mm,
        LAG(valid_time_utc) OVER grid_time AS previous_valid_time_utc,
        LAG(precipitation_mm) OVER grid_time AS previous_precipitation_mm
    FROM {{ ref('archive_hourly') }}
    WINDOW grid_time AS (
        PARTITION BY grid_cell_id
        ORDER BY valid_time_utc
    )
),

run_flags AS (
    SELECT
        *,
        CASE
            WHEN previous_valid_time_utc IS NULL
              OR DATE_DIFF('hour', previous_valid_time_utc, valid_time_utc) <> 1
              OR precipitation_mm IS NULL
              OR previous_precipitation_mm IS NULL
            THEN 1 ELSE 0
        END AS starts_new_observed_run
    FROM lagged
),

observed_runs AS (
    SELECT
        *,
        SUM(starts_new_observed_run) OVER (
            PARTITION BY grid_cell_id
            ORDER BY valid_time_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS observed_run_number
    FROM run_flags
    WHERE precipitation_mm IS NOT NULL
),

run_stats AS (
    SELECT
        grid_cell_id,
        observed_run_number,
        MIN(valid_time_utc) AS run_start_utc,
        MAX(valid_time_utc) AS run_end_utc
    FROM observed_runs
    GROUP BY 1, 2
),

wet_hours AS (
    SELECT
        observed.*,
        stats.run_start_utc,
        stats.run_end_utc,
        LAG(observed.valid_time_utc) OVER (
            PARTITION BY observed.grid_cell_id, observed.observed_run_number
            ORDER BY observed.valid_time_utc
        ) AS previous_wet_time_utc
    FROM observed_runs AS observed
    INNER JOIN run_stats AS stats
        USING (grid_cell_id, observed_run_number)
    WHERE observed.precipitation_mm > 0.1
),

wet_event_flags AS (
    SELECT
        *,
        previous_wet_time_utc IS NULL
            OR DATE_DIFF('hour', previous_wet_time_utc, valid_time_utc) >= 7
            AS starts_new_event
    FROM wet_hours
),

numbered_wet_hours AS (
    SELECT
        *,
        SUM(CAST(starts_new_event AS INTEGER)) OVER (
            PARTITION BY grid_cell_id, observed_run_number
            ORDER BY valid_time_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS event_number_in_run
    FROM wet_event_flags
),

event_bounds AS (
    SELECT
        grid_cell_id,
        weather_model,
        weather_product,
        observed_run_number,
        event_number_in_run,
        MIN(valid_time_utc) AS event_start_utc,
        MAX(valid_time_utc) AS event_end_utc,
        MIN(run_start_utc) AS run_start_utc,
        MAX(run_end_utc) AS run_end_utc,
        MAX(CASE
            WHEN starts_new_event AND previous_wet_time_utc IS NULL
            THEN CAST(DATE_DIFF('hour', run_start_utc, valid_time_utc) < 6 AS INTEGER)
            ELSE 0
        END) = 1 AS is_left_censored
    FROM numbered_wet_hours
    GROUP BY 1, 2, 3, 4, 5
),

bounded_events AS (
    SELECT
        *,
        LEAD(event_start_utc) OVER (
            PARTITION BY grid_cell_id, observed_run_number
            ORDER BY event_number_in_run
        ) AS next_event_start_utc
    FROM event_bounds
)

SELECT
    MD5(CONCAT_WS(
        '|', bounds.grid_cell_id, CAST(bounds.event_start_utc AS VARCHAR),
        'wet_gt_0_1mm_six_dry_hours_v1'
    )) AS rainfall_event_id,
    bounds.grid_cell_id,
    bounds.weather_model,
    bounds.weather_product,
    bounds.event_start_utc,
    bounds.event_end_utc,
    DATE_DIFF('hour', bounds.event_start_utc, bounds.event_end_utc) + 1
        AS event_duration_hours,
    SUM(observed.precipitation_mm) AS event_total_mm,
    COUNT(*) FILTER (WHERE observed.precipitation_mm > 0.1) AS wet_hour_count,
    MAX(observed.precipitation_mm) AS event_peak_1h_mm,
    ARG_MAX(observed.valid_time_utc, observed.precipitation_mm) AS event_peak_1h_time_utc,
    MAX(historical.rain_3h_mm) AS event_peak_3h_mm,
    ARG_MAX(observed.valid_time_utc, historical.rain_3h_mm) AS event_peak_3h_time_utc,
    DATE_DIFF(
        'hour', bounds.event_start_utc,
        ARG_MAX(observed.valid_time_utc, observed.precipitation_mm)
    ) AS time_to_peak_1h_hours,
    bounds.is_left_censored,
    bounds.next_event_start_utc IS NULL
        AND DATE_DIFF('hour', bounds.event_end_utc, bounds.run_end_utc) < 6
        AS is_right_censored,
    'wet_gt_0_1mm_six_dry_hours_v1' AS event_definition_version
FROM bounded_events AS bounds
INNER JOIN observed_runs AS observed
    ON bounds.grid_cell_id = observed.grid_cell_id
   AND bounds.observed_run_number = observed.observed_run_number
   AND observed.valid_time_utc BETWEEN bounds.event_start_utc AND bounds.event_end_utc
LEFT JOIN {{ ref('fct_rainfall_historical_hourly') }} AS historical
    ON observed.grid_cell_id = historical.grid_cell_id
   AND observed.valid_time_utc = historical.valid_time_utc
GROUP BY
    bounds.grid_cell_id,
    bounds.weather_model,
    bounds.weather_product,
    bounds.event_start_utc,
    bounds.event_end_utc,
    bounds.is_left_censored,
    bounds.next_event_start_utc,
    bounds.run_end_utc
