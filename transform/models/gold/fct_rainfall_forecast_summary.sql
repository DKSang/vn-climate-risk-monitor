{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall']) }}

/* GOLD — totals và peaks trong forecast horizon tại as_of của snapshot. */

WITH aggregated AS (
    SELECT
        forecast_snapshot_id,
        as_of_utc,
        grid_cell_id,
        {% for hours in [3, 6, 12, 24, 48] %}
        SUM(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '{{ hours }} hours'
        ) AS rain_next_{{ hours }}h_sum,
        COUNT(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '{{ hours }} hours'
        ) AS rain_next_{{ hours }}h_count{% if not loop.last %},{% endif %}
        {% endfor %},
        MAX(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '6 hours'
        ) AS max_rain_1h_next_6h_raw,
        MAX(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '24 hours'
        ) AS max_rain_1h_next_24h_raw,
        MAX(rain_3h_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '6 hours'
        ) AS max_rain_3h_next_6h_raw,
        MAX(rain_3h_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '24 hours'
        ) AS max_rain_3h_next_24h_raw,
        ARG_MAX(valid_time_utc, precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '24 hours'
        ) AS time_of_max_rain_1h_utc
    FROM {{ ref('fct_rainfall_forecast_hourly') }}
    GROUP BY
        forecast_snapshot_id,
        as_of_utc,
        grid_cell_id
)

SELECT
    forecast_snapshot_id,
    as_of_utc,
    grid_cell_id,
    {% for hours in [3, 6, 12, 24, 48] %}
    CASE WHEN rain_next_{{ hours }}h_count = {{ hours }}
         THEN rain_next_{{ hours }}h_sum END AS forecast_rain_next_{{ hours }}h_mm,
    {% endfor %}
    CASE WHEN rain_next_6h_count = 6
         THEN max_rain_1h_next_6h_raw END AS max_rain_1h_next_6h_mm,
    CASE WHEN rain_next_24h_count = 24
         THEN max_rain_1h_next_24h_raw END AS max_rain_1h_next_24h_mm,
    CASE WHEN rain_next_6h_count = 6
         THEN max_rain_3h_next_6h_raw END AS max_rain_3h_next_6h_mm,
    CASE WHEN rain_next_24h_count = 24
         THEN max_rain_3h_next_24h_raw END AS max_rain_3h_next_24h_mm,
    CASE WHEN rain_next_24h_count = 24
         THEN time_of_max_rain_1h_utc END AS time_of_max_rain_1h_utc,
    CASE
        WHEN rain_next_6h_count <> 6 OR max_rain_1h_next_6h_raw IS NULL THEN NULL
        WHEN max_rain_1h_next_6h_raw > 100 THEN 'over_100'
        WHEN max_rain_1h_next_6h_raw >= 70 THEN 'from_70_to_100'
        WHEN max_rain_1h_next_6h_raw >= 50 THEN 'from_50_to_under_70'
        WHEN max_rain_1h_next_6h_raw >= 0 THEN 'below_50'
    END AS hanoi_rain_scenario_band_next_6h,
    CASE
        WHEN rain_next_24h_count <> 24 OR max_rain_1h_next_24h_raw IS NULL THEN NULL
        WHEN max_rain_1h_next_24h_raw > 100 THEN 'over_100'
        WHEN max_rain_1h_next_24h_raw >= 70 THEN 'from_70_to_100'
        WHEN max_rain_1h_next_24h_raw >= 50 THEN 'from_50_to_under_70'
        WHEN max_rain_1h_next_24h_raw >= 0 THEN 'below_50'
    END AS hanoi_rain_scenario_band_next_24h,
    (
        SELECT MAX(source_object_keys)
        FROM {{ ref('forecast_hourly') }}
    ) AS source_object_keys
FROM aggregated
