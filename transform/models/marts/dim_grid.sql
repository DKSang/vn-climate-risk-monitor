/*
    MART — ô lưới thời tiết. Nơi DUY NHẤT giữ lat/lon của ô.

    Fact chỉ mang `grid_cell_id`. Lặp lat/lon vào fact là mở đường cho hai bảng
    round khác nhau rồi không join được với nhau.
*/

{{ config(
    materialized = 'table',
    tags = ['dim']
) }}

WITH observed AS (
    SELECT
        grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        MIN(valid_time_utc) AS first_observed_utc,
        MAX(valid_time_utc) AS last_observed_utc,
        COUNT(*) AS observation_hours
    FROM {{ ref('int_weather_archive_hourly') }}
    GROUP BY grid_cell_id, weather_model, grid_latitude, grid_longitude
),

-- Độ cao KHÔNG lấy từ bảng theo giờ: response của đợt fetch theo phường trả độ
-- cao của ĐIỂM ĐƯỢC HỎI, nên cùng một ô có nhiều giá trị (đo được 9–41 m).
-- Seed ánh xạ là nơi duy nhất quan hệ phường→ô được ghi tường minh, nên gộp ở
-- đó rồi lấy trung vị làm đại diện cho ô.
elevation AS (
    SELECT grid_cell_id, MEDIAN(grid_elevation_m) AS elevation_m
    FROM {{ ref('stg_seed__ward_grid') }}
    GROUP BY grid_cell_id
)

SELECT
    observed.*,
    elevation.elevation_m
FROM observed
LEFT JOIN elevation USING (grid_cell_id)
