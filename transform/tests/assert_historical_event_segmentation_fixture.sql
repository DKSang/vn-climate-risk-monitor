-- Wet is strictly >0.1; six dry hours (04:00-09:00) split the second event.
WITH fixture AS (
    SELECT
        TIMESTAMPTZ '2026-01-01 00:00:00+00' + i * INTERVAL '1 hour' AS valid_time_utc,
        CASE
            WHEN i = 0 THEN 1.0
            WHEN i = 1 THEN 0.1
            WHEN i = 3 THEN 0.2
            WHEN i = 10 THEN 2.0
            ELSE 0.0
        END AS precipitation_mm
    FROM RANGE(18) AS generated(i)
),

wet_hours AS (
    SELECT
        *,
        LAG(valid_time_utc) OVER (ORDER BY valid_time_utc) AS previous_wet_time_utc
    FROM fixture
    WHERE precipitation_mm > 0.1
),

flags AS (
    SELECT
        *,
        previous_wet_time_utc IS NULL
            OR DATE_DIFF('hour', previous_wet_time_utc, valid_time_utc) >= 7
            AS starts_new_event
    FROM wet_hours
),

numbered AS (
    SELECT
        *,
        SUM(CAST(starts_new_event AS INTEGER)) OVER (ORDER BY valid_time_utc) AS event_number
    FROM flags
),

bounds AS (
    SELECT
        event_number,
        MIN(valid_time_utc) AS event_start_utc,
        MAX(valid_time_utc) AS event_end_utc
    FROM numbered
    GROUP BY 1
),

events AS (
    SELECT
        bounds.event_number,
        bounds.event_start_utc,
        bounds.event_end_utc,
        DATE_DIFF('hour', bounds.event_start_utc, bounds.event_end_utc) + 1 AS duration_hours,
        SUM(fixture.precipitation_mm) AS event_total_mm
    FROM bounds
    INNER JOIN fixture
        ON fixture.valid_time_utc BETWEEN bounds.event_start_utc AND bounds.event_end_utc
    GROUP BY 1, 2, 3
)

SELECT *
FROM events
WHERE (event_number = 1 AND NOT (
          event_start_utc = TIMESTAMPTZ '2026-01-01 00:00:00+00'
          AND event_end_utc = TIMESTAMPTZ '2026-01-01 03:00:00+00'
          AND duration_hours = 4
          AND ABS(event_total_mm - 1.3) < 0.000001
      ))
   OR (event_number = 2 AND NOT (
          event_start_utc = TIMESTAMPTZ '2026-01-01 10:00:00+00'
          AND event_end_utc = TIMESTAMPTZ '2026-01-01 10:00:00+00'
          AND duration_hours = 1
          AND event_total_mm = 2.0
      ))
   OR (SELECT COUNT(*) FROM events) <> 2
