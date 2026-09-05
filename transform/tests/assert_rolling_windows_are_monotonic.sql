-- Cửa sổ rộng hơn phải chứa cửa sổ hẹp hơn, nên tổng của nó không thể nhỏ hơn.
-- Đây là test bắt lỗi RANGE/ROWS hoặc PARTITION sai — dạng lỗi cho ra số hợp lý
-- nhưng SAI, mà mọi test not_null/unique vẫn xanh.
SELECT rain_archive_hourly_key, rain_1h_mm, rain_3h_mm, rain_6h_mm, rain_12h_mm, rain_24h_mm
FROM {{ ref('fct_rain_archive_hourly') }}
WHERE rain_3h_mm  < rain_1h_mm
   OR rain_6h_mm  < rain_3h_mm
   OR rain_12h_mm < rain_6h_mm
   OR rain_24h_mm < rain_12h_mm
