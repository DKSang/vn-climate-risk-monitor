-- Identical grid/month but disjoint model distributions must produce separate quantiles.
WITH fixture AS (
    SELECT 'era5' AS weather_model, 21.0 AS grid_latitude, 105.0 AS grid_longitude,
           CAST(i AS DOUBLE) AS rainfall_mm
    FROM RANGE(1, 101) AS generated(i)
    UNION ALL
    SELECT 'ecmwf_ifs', 21.0, 105.0, 1000.0 + CAST(i AS DOUBLE)
    FROM RANGE(1, 101) AS generated(i)
),

baseline AS (
    SELECT
        weather_model,
        grid_latitude,
        grid_longitude,
        QUANTILE_CONT(rainfall_mm, 0.95) AS p95_rainfall_mm,
        QUANTILE_CONT(rainfall_mm, 0.99) AS p99_rainfall_mm,
        COUNT(*) AS observation_count
    FROM fixture
    GROUP BY 1, 2, 3
)

SELECT *
FROM baseline
WHERE observation_count <> 100
   OR (weather_model = 'era5' AND p99_rainfall_mm >= 1000.0)
   OR (weather_model = 'ecmwf_ifs' AND p95_rainfall_mm <= 1000.0)
   OR (SELECT COUNT(*) FROM baseline) <> 2
