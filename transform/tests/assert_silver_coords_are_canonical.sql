SELECT grid_latitude, grid_longitude
FROM {{ ref('archive_hourly') }}
WHERE ROUND(grid_latitude, 6) IS DISTINCT FROM grid_latitude
   OR ROUND(grid_longitude, 6) IS DISTINCT FROM grid_longitude
UNION ALL
SELECT grid_latitude, grid_longitude
FROM {{ ref('forecast_hourly') }}
WHERE ROUND(grid_latitude, 6) IS DISTINCT FROM grid_latitude
   OR ROUND(grid_longitude, 6) IS DISTINCT FROM grid_longitude
   OR weather_model <> 'ecmwf_ifs'
   OR weather_product <> 'forecast'
