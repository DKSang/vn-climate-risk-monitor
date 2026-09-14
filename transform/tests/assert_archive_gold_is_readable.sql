-- LIMIT 1 buộc engine mở file dữ liệu thật, đồng thời fail nếu archive fact rỗng.
WITH probe AS (
    SELECT CAST(grid_cell_id AS VARCHAR) AS key_value
    FROM {{ ref('fct_rain_archive_hourly') }}
    LIMIT 1
)

SELECT 'fct_rain_archive_hourly' AS relation_name
WHERE NOT EXISTS (SELECT 1 FROM probe WHERE key_value IS NOT NULL)
