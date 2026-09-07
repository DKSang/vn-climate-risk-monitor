/* Current serving view trên bảng forecast-vintage history. */

{{ config(materialized = 'view', tags = ['forecast', 'serving']) }}

WITH latest_run AS (
    SELECT forecast_run_id
    FROM {{ ref('fct_rain_forecast_hourly') }}
    GROUP BY forecast_run_id
    ORDER BY MAX(_ingested_at) DESC, forecast_run_id DESC
    LIMIT 1
)

SELECT history.*
FROM {{ ref('fct_rain_forecast_hourly') }} AS history
JOIN latest_run USING (forecast_run_id)
WHERE history.valid_time_utc >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
