-- Pressure serving data must keep one row per forecast run × ward × hour.
SELECT forecast_run_id, ward_code, valid_time_utc, COUNT(*) AS rows_at_grain
FROM {{ ref('fct_rain_pressure_alert') }}
GROUP BY forecast_run_id, ward_code, valid_time_utc
HAVING COUNT(*) > 1
