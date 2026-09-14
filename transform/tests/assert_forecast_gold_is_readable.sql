-- LIMIT 1 buộc engine mở file dữ liệu thật, đồng thời fail nếu forecast fact rỗng.
WITH probe AS (
    SELECT CAST(rain_forecast_hourly_key AS VARCHAR) AS key_value
    FROM {{ ref('fct_rain_forecast_hourly') }}
    LIMIT 1
)

SELECT 'fct_rain_forecast_hourly' AS relation_name
WHERE NOT EXISTS (SELECT 1 FROM probe WHERE key_value IS NOT NULL)
