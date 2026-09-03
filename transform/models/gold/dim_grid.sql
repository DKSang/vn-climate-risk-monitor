/*
    GOLD — ô lưới thời tiết. Nơi DUY NHẤT giữ lat/lon của ô.

    Fact chỉ mang `grid_cell_id`. Lặp lat/lon vào fact là mở đường cho hai bảng
    round khác nhau rồi không join được với nhau.
*/

{{ config(
    materialized = 'table',
    tags = ['gold', 'dim']
) }}

SELECT
    grid_cell_id,
    weather_model,
    grid_latitude,
    grid_longitude,
    MIN(elevation_m) AS elevation_m,
    MIN(valid_time_utc) AS first_observed_utc,
    MAX(valid_time_utc) AS last_observed_utc,
    COUNT(*) AS observation_hours
FROM {{ ref('weather_hourly') }}
GROUP BY grid_cell_id, weather_model, grid_latitude, grid_longitude
