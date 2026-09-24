-- Mọi ward × giờ của current view phải có pressure cùng forecast run.
SELECT
    forecast.forecast_run,
    bridge.ward_code,
    forecast.valid_at
FROM {{ ref('fct_rain_forecast_current_hourly') }} AS forecast
JOIN {{ ref('bridge_ward_grid') }} AS bridge
    ON bridge.grid_cell_id = forecast.grid_cell_id
   AND bridge.weather_model = forecast.weather_model
LEFT JOIN {{ ref('fct_rain_pressure_alert') }} AS pressure
    ON pressure.forecast_run = forecast.forecast_run
   AND pressure.ward_code = bridge.ward_code
   AND pressure.valid_at = forecast.valid_at
WHERE pressure.ward_code IS NULL
