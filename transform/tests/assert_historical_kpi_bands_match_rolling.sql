SELECT *
FROM {{ ref('fct_rainfall_historical_hourly') }}
WHERE (rain_1h_mm IS NULL) <> (hanoi_rain_scenario_band IS NULL)
   OR (rain_12h_mm IS NULL) <> (vn_rain_band_12h IS NULL)
   OR (rain_24h_mm IS NULL) <> (vn_rain_band_24h IS NULL)
   OR (
        rain_12h_mm IS NOT NULL
        AND rain_24h_mm IS NOT NULL
        AND vn_rain_threshold_exceeded IS DISTINCT FROM (
            rain_12h_mm >= 50 OR rain_24h_mm >= 100
        )
    )
