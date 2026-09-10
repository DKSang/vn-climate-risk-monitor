-- LIMIT 1 buộc engine mở file dữ liệu thật, đồng thời fail nếu core fact rỗng.
WITH archive_probe AS (
    SELECT CAST(grid_cell_id AS VARCHAR) AS key_value
    FROM {{ ref('fct_rain_archive_hourly') }}
    LIMIT 1
),
forecast_probe AS (
    SELECT CAST(rain_forecast_hourly_key AS VARCHAR) AS key_value
    FROM {{ ref('fct_rain_forecast_hourly') }}
    LIMIT 1
)

SELECT 'fct_rain_archive_hourly' AS relation_name
WHERE NOT EXISTS (SELECT 1 FROM archive_probe WHERE key_value IS NOT NULL)
UNION ALL
SELECT 'fct_rain_forecast_hourly' AS relation_name
WHERE NOT EXISTS (SELECT 1 FROM forecast_probe WHERE key_value IS NOT NULL)
