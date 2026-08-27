-- Fixture biết trước: đủ 3 giờ => 6 mm; gap hoặc NULL => incomplete/NULL.
WITH fixture(grid_cell_id, valid_time_utc, precipitation_mm) AS (
    VALUES
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', 1.0),
        ('complete', TIMESTAMPTZ '2026-01-01 01:00:00+00', 2.0),
        ('complete', TIMESTAMPTZ '2026-01-01 02:00:00+00', 3.0),
        ('gap',      TIMESTAMPTZ '2026-01-01 00:00:00+00', 1.0),
        ('gap',      TIMESTAMPTZ '2026-01-01 01:00:00+00', 2.0),
        ('gap',      TIMESTAMPTZ '2026-01-01 03:00:00+00', 4.0),
        ('null',     TIMESTAMPTZ '2026-01-01 00:00:00+00', 1.0),
        ('null',     TIMESTAMPTZ '2026-01-01 01:00:00+00', NULL),
        ('null',     TIMESTAMPTZ '2026-01-01 02:00:00+00', 3.0)
),

actual AS (
    SELECT
        *,
        SUM(precipitation_mm) OVER (
            PARTITION BY grid_cell_id
            ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '2 hours' PRECEDING AND CURRENT ROW
        ) AS rain_sum,
        COUNT(precipitation_mm) OVER (
            PARTITION BY grid_cell_id
            ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '2 hours' PRECEDING AND CURRENT ROW
        ) AS coverage_count
    FROM fixture
),

result AS (
    SELECT
        grid_cell_id,
        CASE WHEN coverage_count = 3 THEN rain_sum END AS rain_3h_mm,
        coverage_count / 3.0 AS coverage_ratio,
        coverage_count = 3 AS is_complete
    FROM actual
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY grid_cell_id ORDER BY valid_time_utc DESC
    ) = 1
)

SELECT *
FROM result
WHERE (grid_cell_id = 'complete' AND NOT (
          rain_3h_mm = 6.0 AND coverage_ratio = 1.0 AND is_complete
      ))
   OR (grid_cell_id IN ('gap', 'null') AND NOT (
          rain_3h_mm IS NULL AND coverage_ratio < 1.0 AND NOT is_complete
      ))
