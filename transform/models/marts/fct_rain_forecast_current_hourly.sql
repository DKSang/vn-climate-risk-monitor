/* Current serving view trên bảng forecast-vintage history. */

{{ config(materialized = 'view', tags = ['forecast', 'serving']) }}

WITH latest_run AS (
    SELECT weather_model, forecast_run_id
    FROM {{ ref('fct_rain_forecast_hourly') }}
    WHERE weather_model = '{{ var('forecast_model', 'ecmwf_ifs') }}'
    GROUP BY weather_model, forecast_run_id
    ORDER BY {{ forecast_run_order() }}
    LIMIT 1
)

SELECT history.*
FROM {{ ref('fct_rain_forecast_hourly') }} AS history
JOIN latest_run USING (weather_model, forecast_run_id)
WHERE history.valid_time_utc >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
