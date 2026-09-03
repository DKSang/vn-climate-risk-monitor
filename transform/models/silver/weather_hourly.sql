/*
    SILVER — dedup và conform quan trắc theo giờ. KHÔNG business logic.

    Hai việc, đúng hai việc:

    1. DEDUP. Bronze là append-only và at-least-once: autoloader có thể INSERT
       lại cùng một object khi lô trước chết giữa chừng (Bronze commit xong
       nhưng file chưa kịp COMMITTED). Giữ lần ingest MỚI NHẤT của mỗi grain.

    2. CANONICAL toạ độ. Round 6 số ĐÚNG MỘT LẦN ở đây rồi sinh `grid_cell_id`.
       Mọi lớp sau chỉ dùng id, không bao giờ round lại — round hai lần ở hai
       chỗ là cách chắc chắn nhất để hai bảng không join được với nhau.
*/

{{ config(materialized = 'view') }}

WITH canonical AS (
    SELECT
        weather_model,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        elevation_m,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at
    FROM {{ source('silver_staging', 'stg_weather_hourly') }}
    WHERE valid_time_utc IS NOT NULL
)

SELECT
    {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }}
        AS grid_cell_id,
    weather_model,
    grid_latitude,
    grid_longitude,
    elevation_m,
    valid_time_utc,
    precipitation_mm,
    rain_mm,
    weather_code,
    soil_moisture_0_to_7cm,
    soil_moisture_7_to_28cm,
    _source_file,
    _ingested_at
FROM canonical
-- Grain là (model, ô, giờ). Cùng grain đến từ nhiều file thì bản nạp sau thắng:
-- nó là kết quả của lần fetch mới nhất cho đúng ô và giờ đó.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY weather_model, grid_latitude, grid_longitude, valid_time_utc
    ORDER BY _ingested_at DESC, _source_file DESC
) = 1
