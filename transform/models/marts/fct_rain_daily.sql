/*
    MART — mưa theo ô lưới × ngày.

    Full refresh: dựng từ `fct_rain_hourly` đã tính sẵn cửa sổ trượt, ~9,7k
    ngày × 60 ô nên rebuild rẻ hơn nhiều so với nuôi thêm một checkpoint.

    Ngày theo UTC, khớp `valid_time_utc`. Đổi sang giờ VN là quyết định của lớp
    phục vụ, không phải của fact — đổi ở đây làm mọi so sánh với chuỗi lịch sử
    lệch 7 giờ mà không ai thấy.
*/

{{ config(
    materialized = 'table',
    tags = ['fact', 'rain']
) }}

{% set windows = rain_windows() %}

SELECT
    MD5(CONCAT_WS('|', grid_cell_id, CAST(rain_date AS VARCHAR)))
        AS rain_daily_key,
    grid_cell_id,
    rain_date,
    SUM(precipitation_mm) AS rain_total_mm,
    COUNT(precipitation_mm) AS observed_hours,
    -- Ngày đủ 24 giờ mới so được với ngày khác; thiếu giờ thì tổng nhỏ giả tạo.
    COUNT(precipitation_mm) = 24 AS is_complete_day,
    {%- for hours in windows %}
    MAX(rain_{{ hours }}h_mm) AS peak_rain_{{ hours }}h_mm,
    {%- endfor %}
    -- Số giờ chạm từng dải kịch bản Hà Nội — đầu vào trực tiếp cho Q7.
    COUNT(*) FILTER (hanoi_rain_scenario_band = 'from_50_to_under_70')
        AS hours_rain_50_to_70,
    COUNT(*) FILTER (hanoi_rain_scenario_band = 'from_70_to_100')
        AS hours_rain_70_to_100,
    COUNT(*) FILTER (hanoi_rain_scenario_band = 'over_100')
        AS hours_rain_over_100,
    MAX(_ingested_at) AS _ingested_at
FROM {{ ref('fct_rain_hourly') }}
GROUP BY grid_cell_id, rain_date
