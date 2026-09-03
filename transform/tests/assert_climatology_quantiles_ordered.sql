SELECT *
FROM {{ ref('fct_rainfall_climatology_monthly') }}
WHERE observation_count < 1
   OR median_rainfall_mm > p95_rainfall_mm
   OR p95_rainfall_mm > p99_rainfall_mm
   OR baseline_first_observation_utc > baseline_last_observation_utc
