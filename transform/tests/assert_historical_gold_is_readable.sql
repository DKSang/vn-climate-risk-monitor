-- Buộc đọc Parquet, không tin COUNT(*) từ metadata. Archive rỗng = bước 5 chưa xong.
SELECT
    COUNT(DISTINCT historical_hourly_key) AS hourly_keys,
    COUNT(DISTINCT weather_model) AS model_count
FROM {{ ref('fct_rainfall_historical_hourly') }}
HAVING
    COUNT(DISTINCT historical_hourly_key) < 1
    OR COUNT(DISTINCT weather_model) < 1
