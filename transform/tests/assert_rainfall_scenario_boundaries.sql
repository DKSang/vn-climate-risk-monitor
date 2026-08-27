WITH fixture(rain_1h_mm, expected_band) AS (
    VALUES
        (0.0,      'below_50'),
        (49.9999,  'below_50'),
        (50.0,     'from_50_to_under_70'),
        (69.9999,  'from_50_to_under_70'),
        (70.0,     'from_70_to_100'),
        (100.0,    'from_70_to_100'),
        (100.0001, 'over_100')
)

SELECT *
FROM fixture
WHERE expected_band <> CASE
    WHEN rain_1h_mm > 100 THEN 'over_100'
    WHEN rain_1h_mm >= 70 THEN 'from_70_to_100'
    WHEN rain_1h_mm >= 50 THEN 'from_50_to_under_70'
    WHEN rain_1h_mm >= 0 THEN 'below_50'
END
