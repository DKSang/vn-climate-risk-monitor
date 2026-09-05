-- Grain của lớp curated là (model, ô, giờ). Dedup hỏng thì Gold nhân đôi lượng
-- mưa mà không test nào khác thấy: unique key của Gold vẫn duy nhất vì nó dựng
-- từ chính grain đã nhân đôi.
SELECT grid_cell_id, valid_time_utc, COUNT(*) AS rows_at_grain
FROM {{ ref('int_weather_archive_hourly') }}
GROUP BY grid_cell_id, valid_time_utc
HAVING COUNT(*) > 1
