WITH invalid_keys AS (
    SELECT 'int_weather_archive_hourly' AS model_name
    FROM {{ ref('int_weather_archive_hourly') }}
    WHERE weather_archive_hourly_key <> MD5(CONCAT_WS(
        '|', grid_cell_id, CAST(EPOCH_US(valid_time_utc) AS VARCHAR)
    ))

    UNION ALL

    SELECT 'int_weather_forecast_hourly'
    FROM {{ ref('int_weather_forecast_hourly') }}
    WHERE weather_forecast_hourly_key <> MD5(CONCAT_WS(
        '|', forecast_run_id, grid_cell_id,
        CAST(EPOCH_US(valid_time_utc) AS VARCHAR)
    ))

    UNION ALL

    SELECT 'fct_rain_archive_hourly'
    FROM {{ ref('fct_rain_archive_hourly') }}
    WHERE rain_archive_hourly_key <> MD5(CONCAT_WS(
        '|', grid_cell_id, CAST(EPOCH_US(valid_time_utc) AS VARCHAR)
    ))

    UNION ALL

    SELECT 'fct_rain_forecast_hourly'
    FROM {{ ref('fct_rain_forecast_hourly') }}
    WHERE rain_forecast_hourly_key <> MD5(CONCAT_WS(
        '|', forecast_run_id, grid_cell_id,
        CAST(EPOCH_US(valid_time_utc) AS VARCHAR)
    ))

    UNION ALL

    SELECT 'fct_rain_pressure_alert'
    FROM {{ ref('fct_rain_pressure_alert') }}
    WHERE rain_pressure_alert_key <> MD5(CONCAT_WS(
        '|', forecast_run_id, ward_code,
        CAST(EPOCH_US(valid_time_utc) AS VARCHAR)
    ))
)

SELECT model_name, COUNT(*) AS invalid_key_count
FROM invalid_keys
GROUP BY model_name
