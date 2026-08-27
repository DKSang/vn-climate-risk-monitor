{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall']) }}

/*
    GOLD — rolling rainfall trên một forecast snapshot hoàn chỉnh.

    RANGE theo timestamp thay vì ROWS: thiếu một giờ làm COUNT < H và KPI NULL,
    dù vẫn có đủ H dòng ở xa hơn trong lịch sử. Không bao giờ biến NULL thành 0.
*/

WITH windowed AS (
    SELECT
        forecast_snapshot_id,
        as_of_utc,
        grid_cell_id,
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        precipitation_mm,
        {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
        SUM(precipitation_mm) OVER (
            PARTITION BY forecast_snapshot_id, grid_cell_id
            ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
        ) AS rain_{{ hours }}h_sum,
        COUNT(precipitation_mm) OVER (
            PARTITION BY forecast_snapshot_id, grid_cell_id
            ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
        ) AS rain_{{ hours }}h_count{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ ref('forecast_hourly') }}
)

SELECT
    forecast_snapshot_id,
    as_of_utc,
    grid_cell_id,
    grid_latitude,
    grid_longitude,
    valid_time_utc,
    precipitation_mm,
    {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
    CASE WHEN rain_{{ hours }}h_count = {{ hours }}
         THEN rain_{{ hours }}h_sum END AS rain_{{ hours }}h_mm,
    {% endfor %}
    CASE
        WHEN rain_1h_count <> 1 THEN NULL
        WHEN rain_1h_sum > 100 THEN 'over_100'
        WHEN rain_1h_sum >= 70 THEN 'from_70_to_100'
        WHEN rain_1h_sum >= 50 THEN 'from_50_to_under_70'
        WHEN rain_1h_sum >= 0 THEN 'below_50'
    END AS hanoi_rain_scenario_band
FROM windowed
