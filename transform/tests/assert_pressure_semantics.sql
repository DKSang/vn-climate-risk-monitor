-- Scores stay in 0-100; NORMAL needs complete inputs; no coverage means UNKNOWN with no score.
SELECT *
FROM {{ ref('fct_rain_pressure_alert') }}
WHERE pressure_score < 0
   OR pressure_score > 100
   OR (pressure_level = 'NORMAL' AND coverage_status <> 'COMPLETE')
   OR (coverage_status = 'NONE' AND pressure_level <> 'UNKNOWN')
   OR (coverage_status = 'NONE' AND pressure_score IS NOT NULL)
   OR (coverage_status <> 'NONE' AND pressure_score IS NULL)
   OR (revision_direction = 'UNKNOWN' AND forecast_next_24h_mm IS NOT NULL)
   OR (revision_direction <> 'UNKNOWN' AND forecast_next_24h_mm IS NULL)
