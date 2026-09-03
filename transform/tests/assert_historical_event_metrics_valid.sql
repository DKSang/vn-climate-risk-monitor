SELECT *
FROM {{ ref('fct_rainfall_historical_event') }}
WHERE event_end_utc < event_start_utc
   OR event_duration_hours <> DATE_DIFF('hour', event_start_utc, event_end_utc) + 1
   OR event_duration_hours < 1
   OR wet_hour_count < 1
   OR event_total_mm <= 0.1
   OR event_peak_1h_mm <= 0.1
   OR time_to_peak_1h_hours < 0
   OR time_to_peak_1h_hours >= event_duration_hours
