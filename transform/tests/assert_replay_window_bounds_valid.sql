SELECT *
FROM {{ ref('dim_historical_replay_window') }}
WHERE window_start_utc >= window_end_utc
   OR DATE_DIFF('hour', window_start_utc, window_end_utc) < 1
