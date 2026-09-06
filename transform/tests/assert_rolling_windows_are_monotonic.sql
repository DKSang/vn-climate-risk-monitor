-- Cửa sổ rộng hơn phải chứa cửa sổ hẹp hơn, nên tổng của nó không thể nhỏ hơn.
-- Đây là test bắt lỗi RANGE/ROWS hoặc PARTITION sai — dạng lỗi cho ra số hợp lý
-- nhưng SAI, mà mọi test not_null/unique vẫn xanh.
--
-- Cặp so sánh sinh TỪ `rain_windows()`, không liệt kê tay: thêm một cửa sổ vào
-- macro mà quên sửa file này thì cửa sổ mới sẽ không được kiểm chứng gì cả.
{% set windows = rain_windows() %}

SELECT
    rain_archive_hourly_key,
    {%- for hours in windows %}
    rain_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ ref('fct_rain_archive_hourly') }}
WHERE FALSE
    {%- for hours in windows %}
    {%- if not loop.first %}
    OR rain_{{ hours }}h_mm < rain_{{ windows[loop.index0 - 1] }}h_mm
    {%- endif %}
    {%- endfor %}
