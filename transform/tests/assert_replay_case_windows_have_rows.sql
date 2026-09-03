-- Bộ case §9 trong docs/01-business-problem.md phải có hourly rows sau khi archive có dữ liệu.
WITH expected(replay_window_id) AS (
    VALUES
        ('hanoi_historic_rain_2008'),
        ('typhoon_yagi_2024'),
        ('late_august_heavy_rain_2025'),
        ('widespread_flood_2025_10_08')
)

SELECT expected.replay_window_id
FROM expected
LEFT JOIN {{ ref('fct_ward_historical_replay_hourly') }} AS replay
    USING (replay_window_id)
GROUP BY expected.replay_window_id
HAVING COUNT(replay.ward_replay_hourly_key) = 0
