-- Cửa sổ rộng hơn phải chứa cửa sổ hẹp hơn, nên tổng của nó không thể nhỏ hơn.
-- Kiểm tra fct_rain_forecast_hourly tương tự như fct_rain_archive_hourly, và
-- cũng sinh cặp so sánh từ `rain_windows()` thay vì liệt kê tay.
{% set windows = rain_windows() %}

SELECT
    rain_forecast_hourly_key,
    {%- for hours in windows %}
    rain_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ ref('fct_rain_forecast_hourly') }}
WHERE FALSE
    {%- for hours in windows %}
    {%- if not loop.first %}
    OR rain_{{ hours }}h_mm < rain_{{ windows[loop.index0 - 1] }}h_mm
    {%- endif %}
    {%- endfor %}
