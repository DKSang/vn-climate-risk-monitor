SELECT
    left_window.replay_window_id AS left_replay_window_id,
    right_window.replay_window_id AS right_replay_window_id
FROM {{ ref('dim_historical_replay_window') }} AS left_window
INNER JOIN {{ ref('dim_historical_replay_window') }} AS right_window
    ON left_window.replay_window_id < right_window.replay_window_id
   AND left_window.window_start_utc < right_window.window_end_utc
   AND right_window.window_start_utc < left_window.window_end_utc
