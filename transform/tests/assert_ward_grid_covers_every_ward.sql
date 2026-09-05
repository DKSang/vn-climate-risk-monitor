-- Mỗi phường phải có đúng một ô cho MỖI model. Thiếu một dòng thì phường đó
-- biến mất khỏi fct_ward_rain_archive_daily một cách im lặng — INNER JOIN không báo gì.
SELECT ward.ward_code, model.weather_model, COUNT(map.grid_cell_id) AS mappings
FROM {{ ref('dim_ward') }} AS ward
CROSS JOIN (SELECT DISTINCT weather_model FROM {{ ref('dim_grid') }}) AS model
LEFT JOIN {{ ref('bridge_ward_grid') }} AS map
       ON map.ward_code = ward.ward_code
      AND map.weather_model = model.weather_model
GROUP BY ward.ward_code, model.weather_model
HAVING COUNT(map.grid_cell_id) <> 1
