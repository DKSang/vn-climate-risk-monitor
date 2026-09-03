-- Complete series returns every rolling total; an old gap invalidates 72h, not 24h.
WITH complete AS (
    SELECT
        'complete' AS series_id,
        TIMESTAMPTZ '2026-01-01 00:00:00+00' + i * INTERVAL '1 hour' AS valid_time_utc,
        1.0 AS precipitation_mm
    FROM RANGE(72) AS generated(i)
),

gapped AS (
    SELECT
        'gapped' AS series_id,
        TIMESTAMPTZ '2026-01-01 00:00:00+00' + i * INTERVAL '1 hour' AS valid_time_utc,
        1.0 AS precipitation_mm
    FROM RANGE(72) AS generated(i)
    WHERE i <> 40
),

fixture AS (
    SELECT * FROM complete
    UNION ALL
    SELECT * FROM gapped
),

windowed AS (
    SELECT
        *,
        {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
        SUM(precipitation_mm) OVER (
            PARTITION BY series_id ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
        ) AS rain_{{ hours }}h_sum,
        COUNT(precipitation_mm) OVER (
            PARTITION BY series_id ORDER BY valid_time_utc
            RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
        ) AS rain_{{ hours }}h_count{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM fixture
),

last_hour AS (
    SELECT *
    FROM windowed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY valid_time_utc DESC) = 1
)

SELECT *
FROM last_hour
WHERE (series_id = 'complete' AND NOT (
    {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
    rain_{{ hours }}h_count = {{ hours }} AND rain_{{ hours }}h_sum = {{ hours }}.0{% if not loop.last %} AND{% endif %}
    {% endfor %}
))
OR (series_id = 'gapped' AND NOT (
    rain_24h_count = 24 AND rain_24h_sum = 24.0 AND rain_72h_count = 71
))
