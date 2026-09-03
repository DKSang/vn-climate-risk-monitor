SELECT *
FROM {{ ref('fct_ward_historical_replay_hourly') }}
WHERE valid_time_utc < window_start_utc
   OR valid_time_utc >= window_end_utc
