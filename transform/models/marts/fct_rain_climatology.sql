/*
    MART — phân phối lịch sử của mưa và độ ẩm đất, theo ô lưới × tháng lịch.

    Grain: (grid_cell_id, calendar_month).

    Đây là MẪU SỐ để mọi feature chuẩn hoá về 0–1. Một giá trị mưa thô không tự
    nói lên điều gì: 60 mm/24h là chuyện thường vào tháng 8 và là cực đoan vào
    tháng 1, và ô ngoại thành khác ô nội thành.

    TÁCH THEO `weather_model`, không trộn: ERA5 (0,25°, trước 2017) và ECMWF IFS
    (~9 km, từ 2017) có phân phối mưa khác nhau, và ô lưới của hai model không
    trùng nhau — nên `grid_cell_id` đã ngầm tách sẵn theo model. docs/05 §6.

    Phân vị tính trên TOÀN BỘ giờ, kể cả giờ khô. Mưa lệch phải và có rất nhiều
    số 0, nên p50 hầu như luôn bằng 0 — đó là lý do dùng p95/p99 làm trần chuẩn
    hoá chứ không dùng trung bình hay độ lệch chuẩn.
*/

{{ config(
    materialized = 'table',
    tags = ['fact', 'rain']
) }}

{% set windows = rain_windows() %}

SELECT
    grid_cell_id,
    MONTH(valid_time_utc) AS calendar_month,
    COUNT(*) AS observation_count,
    MIN(valid_time_utc) AS baseline_starts_at_utc,
    MAX(valid_time_utc) AS baseline_ends_at_utc,
    {%- for hours in windows %}
    QUANTILE_CONT(rain_{{ hours }}h_mm, 0.95) AS rain_{{ hours }}h_p95_mm,
    QUANTILE_CONT(rain_{{ hours }}h_mm, 0.99) AS rain_{{ hours }}h_p99_mm,
    {%- endfor %}
    QUANTILE_CONT(soil_moisture_0_to_7cm, 0.10) AS soil_moisture_p10,
    QUANTILE_CONT(soil_moisture_0_to_7cm, 0.90) AS soil_moisture_p90,
    -- Version của định nghĩa baseline, để KPI cũ còn đối chiếu được về sau.
    'all_available_by_grid_month_v1' AS climatology_window_id,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('fct_rain_archive_hourly') }}
GROUP BY grid_cell_id, MONTH(valid_time_utc)
