-- Mọi ward × giờ của current view phải có pressure cùng forecast run.
SELECT
    forecast.forecast_run_id,
    bridge.ward_code,
    forecast.valid_time_utc
FROM {{ ref('fct_rain_forecast_current_hourly') }} AS forecast
JOIN {{ ref('bridge_ward_grid') }} AS bridge
    ON bridge.grid_cell_id = forecast.grid_cell_id
   AND bridge.weather_model = forecast.weather_model
   AND bridge.is_active = TRUE
LEFT JOIN {{ ref('fct_rain_pressure_alert') }} AS pressure
    ON pressure.forecast_run_id = forecast.forecast_run_id
   AND pressure.ward_code = bridge.ward_code
   AND pressure.valid_time_utc = forecast.valid_time_utc
WHERE pressure.rain_pressure_alert_key IS NULL
