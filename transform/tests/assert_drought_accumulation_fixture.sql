-- A missing calendar day must invalidate 30/60/90-day accumulation as applicable.
WITH complete AS (
    SELECT
        'complete' AS series_id,
        DATE '2026-01-01' + CAST(i AS INTEGER) AS rainfall_date,
        1.0 AS daily_rainfall_mm
    FROM RANGE(90) AS generated(i)
),

gapped AS (
    SELECT
        'gapped' AS series_id,
        DATE '2026-01-01' + CAST(i AS INTEGER) AS rainfall_date,
        1.0 AS daily_rainfall_mm
    FROM RANGE(90) AS generated(i)
    WHERE i <> 30
),

fixture AS (
    SELECT * FROM complete
    UNION ALL
    SELECT * FROM gapped
),

rolling AS (
    SELECT
        *,
        {% for days in [30, 60, 90] %}
        SUM(daily_rainfall_mm) OVER (
            PARTITION BY series_id ORDER BY rainfall_date
            RANGE BETWEEN INTERVAL '{{ days - 1 }} days' PRECEDING AND CURRENT ROW
        ) AS rain_{{ days }}d_sum,
        COUNT(daily_rainfall_mm) OVER (
            PARTITION BY series_id ORDER BY rainfall_date
            RANGE BETWEEN INTERVAL '{{ days - 1 }} days' PRECEDING AND CURRENT ROW
        ) AS rain_{{ days }}d_count{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM fixture
),

last_day AS (
    SELECT *
    FROM rolling
    QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY rainfall_date DESC) = 1
)

SELECT *
FROM last_day
WHERE (series_id = 'complete' AND NOT (
          rain_30d_count = 30 AND rain_30d_sum = 30.0
          AND rain_60d_count = 60 AND rain_60d_sum = 60.0
          AND rain_90d_count = 90 AND rain_90d_sum = 90.0
      ))
   OR (series_id = 'gapped' AND NOT (
          rain_30d_count = 30
          AND rain_60d_count = 59
          AND rain_90d_count = 89
      ))
