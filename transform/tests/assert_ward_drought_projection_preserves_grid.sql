SELECT ward.*
FROM {{ ref('fct_ward_rainfall_drought_daily') }} AS ward
INNER JOIN {{ ref('fct_rainfall_drought_daily') }} AS grid
    ON ward.drought_daily_key = grid.drought_daily_key
WHERE ward.daily_rainfall_mm IS DISTINCT FROM grid.daily_rainfall_mm
   OR ward.rain_30d_mm IS DISTINCT FROM grid.rain_30d_mm
   OR ward.rain_60d_mm IS DISTINCT FROM grid.rain_60d_mm
   OR ward.rain_90d_mm IS DISTINCT FROM grid.rain_90d_mm
