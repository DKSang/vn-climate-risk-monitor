/*
    STAGING — nguồn open_meteo archive, hợp đồng một-một với bảng vật lý
    `silver.stg_weather_hourly` mà autoloader ghi (ngoài đồ thị dbt).

    MỎNG CÓ CHỦ ĐÍCH: chỉ chọn cột, đổi tên nếu cần, và loại dòng không có
    `valid_time_utc` (dòng đó không có grain, giữ lại chỉ để làm mọi lớp sau
    phải kiểm tra null). KHÔNG ở đây:

      - dedup (ROW_NUMBER) — đó là việc của `int_weather_hourly`. Đẩy window
        function lên view staging sẽ tái tạo đúng bug đã đo: DuckDB không đẩy
        filter xuống dưới window function, 9,7s/lọc qua view so với 0,0s qua
        table, và macro incremental gọi subquery đó nhiều lần mỗi run.
      - canonical toạ độ (ROUND 6) — làm một lần ở intermediate, sát chỗ sinh
        key hơn.
      - bộ lọc incremental — thuộc về consumer nào đọc theo watermark.

    Append-only staging giữ MỌI phiên bản đã fetch (70% trùng là change log hợp
    lệ); mọi phiên bản đi qua đây nguyên vẹn.
*/

{{ config(materialized = 'view') }}

SELECT
    weather_model,
    grid_latitude,
    grid_longitude,
    elevation_m,
    valid_time_utc,
    precipitation_mm,
    rain_mm,
    weather_code,
    soil_moisture_0_to_7cm,
    soil_moisture_7_to_28cm,
    timezone,
    utc_offset_seconds,
    _source_file,
    _ingested_at
FROM {{ source('silver_staging', 'stg_weather_hourly') }}
WHERE valid_time_utc IS NOT NULL
