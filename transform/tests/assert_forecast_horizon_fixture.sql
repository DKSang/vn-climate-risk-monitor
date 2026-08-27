-- as_of bị loại khỏi horizon: next 3h = 1+2+3, next 6h = 1+...+6.
WITH fixture(grid_cell_id, as_of_utc, valid_time_utc, precipitation_mm) AS (
    VALUES
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 00:00:00+00', 99.0),
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 01:00:00+00', 1.0),
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 02:00:00+00', 2.0),
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 03:00:00+00', 3.0),
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 04:00:00+00', 4.0),
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 05:00:00+00', 5.0),
        ('complete', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 06:00:00+00', 6.0),
        ('missing',  TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 01:00:00+00', 1.0),
        ('missing',  TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 03:00:00+00', 3.0)
),

actual AS (
    SELECT
        grid_cell_id,
        SUM(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '3 hours'
        ) AS next_3h_sum,
        COUNT(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '3 hours'
        ) AS next_3h_count,
        SUM(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '6 hours'
        ) AS next_6h_sum,
        MAX(precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '6 hours'
        ) AS peak_1h_next_6h,
        ARG_MAX(valid_time_utc, precipitation_mm) FILTER (
            WHERE valid_time_utc > as_of_utc
              AND valid_time_utc <= as_of_utc + INTERVAL '6 hours'
        ) AS peak_time
    FROM fixture
    GROUP BY grid_cell_id
)

SELECT *
FROM actual
WHERE (grid_cell_id = 'complete' AND NOT (
          next_3h_count = 3
          AND next_3h_sum = 6.0
          AND next_6h_sum = 21.0
          AND peak_1h_next_6h = 6.0
          AND peak_time = TIMESTAMPTZ '2026-01-01 06:00:00+00'
      ))
   OR (grid_cell_id = 'missing' AND next_3h_count = 3)
