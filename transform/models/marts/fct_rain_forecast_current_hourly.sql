/* The latest forecast run, from the current hour on. */

{{ config(materialized = 'view', tags = ['forecast']) }}

SELECT *
FROM {{ ref('fct_rain_forecast_hourly') }}
WHERE forecast_run = (SELECT MAX(forecast_run) FROM {{ ref('fct_rain_forecast_hourly') }})
  AND valid_at >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
