-- Điều 44 QĐ 18/2021/QĐ-TTg: feature dải mưa 12/24h, không suy ra cấp độ pháp lý.
WITH fixture(
    rain_12h_mm, rain_24h_mm,
    expected_12h, expected_24h, expected_exceeded
) AS (
    VALUES
        (49.9999,  99.9999,  'below_50',        'below_100',         FALSE),
        (50.0,     100.0,    'from_50_to_100', 'from_100_to_200',   TRUE),
        (100.0,    200.0,    'from_50_to_100', 'from_100_to_200',   TRUE),
        (100.0001, 200.0001, 'over_100',        'over_200_to_400',  TRUE),
        (0.0,      400.0,    'below_50',        'over_200_to_400',  TRUE),
        (0.0,      400.0001, 'below_50',        'over_400',         TRUE)
)

SELECT *
FROM fixture
WHERE expected_12h <> CASE
        WHEN rain_12h_mm > 100 THEN 'over_100'
        WHEN rain_12h_mm >= 50 THEN 'from_50_to_100'
        WHEN rain_12h_mm >= 0 THEN 'below_50'
    END
   OR expected_24h <> CASE
        WHEN rain_24h_mm > 400 THEN 'over_400'
        WHEN rain_24h_mm > 200 THEN 'over_200_to_400'
        WHEN rain_24h_mm >= 100 THEN 'from_100_to_200'
        WHEN rain_24h_mm >= 0 THEN 'below_100'
    END
   OR expected_exceeded IS DISTINCT FROM (
        rain_12h_mm >= 50 OR rain_24h_mm >= 100
    )
