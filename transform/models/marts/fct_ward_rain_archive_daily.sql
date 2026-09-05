/*
    MART — mưa theo phường × ngày (Archive). Chiếu `fct_rain_archive_daily` qua bridge.

    Vì sao chỉ ở grain NGÀY: 126 phường chỉ có 12 (era5) hoặc 48 (ecmwf_ifs) giá
    trị khác nhau ở mỗi giờ. Một fact phường × giờ sẽ là ~26 triệu dòng mà phần
    lớn là bản sao của nhau. Câu hỏi theo giờ ở cấp phường join
    `bridge_ward_grid` (252 dòng) với `fct_rain_archive_hourly` lúc query — rẻ hơn và
    không có bảng nào phải giữ đồng bộ.

    KHÔNG khử trùng ở đây: mỗi phường một dòng là đúng grain. Việc "không nhân
    bản trọng số khi nhiều phường chung ô" (Q4) dùng `ward_count_on_grid` ở
    bridge, không phải bằng cách bỏ bớt dòng.
*/

{{ config(
    materialized = 'table',
    tags = ['fact', 'rain', 'ward']
) }}

{% set windows = rain_windows() %}

SELECT
    MD5(CONCAT_WS(
        '|', bridge.ward_code, bridge.weather_model,
        CAST(daily.rain_date AS VARCHAR)
    )) AS ward_rain_archive_daily_key,
    bridge.ward_code,
    bridge.weather_model,
    bridge.grid_cell_id,
    bridge.ward_count_on_grid,
    daily.rain_date,
    daily.rain_total_mm,
    daily.observed_hours,
    daily.is_complete_day,
    {%- for hours in windows %}
    daily.peak_rain_{{ hours }}h_mm,
    {%- endfor %}
    daily.hours_rain_50_to_70,
    daily.hours_rain_70_to_100,
    daily.hours_rain_over_100,
    bridge.is_active AS ward_is_active,
    bridge._deactivated_at AS ward_grid_deactivated_at
FROM {{ ref('fct_rain_archive_daily') }} AS daily
INNER JOIN {{ ref('bridge_ward_grid') }} AS bridge
    ON bridge.grid_cell_id = daily.grid_cell_id
