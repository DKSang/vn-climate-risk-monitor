{#
    Cửa sổ trượt và ngưỡng nghiệp vụ — khai báo MỘT chỗ.

    Bản cũ nhân bản logic này trong từng model, và mỗi cửa sổ sinh ba cột
    (`_mm`, `_coverage_ratio`, `_is_complete`) × 7 cửa sổ = 21 cột. Thêm một
    cửa sổ phải sửa ba model. Ở đây thêm cửa sổ = sửa một danh sách.
#}

{#
    Cửa sổ của phase hiện tại.

    Chỉ giữ 1/3/6/12/24 giờ vì đây là các cửa sổ có consumer trong dashboard và
    pressure signal.

    Phải là MACRO chứ không phải `{% set %}` ở cấp file: dbt chỉ export block
    `macro` từ macro-paths, biến top-level không nhìn thấy được từ model.

    Lookback incremental của các fact hourly SUY RA từ danh sách này
    (max(windows) − 1 giờ), không phải gõ tay — xem `{% set lookback %}`.
#}
{% macro rain_windows() %}
    {{ return([1, 3, 6, 12, 24]) }}
{% endmacro %}


{#
    Tổng mưa trượt cho mỗi cửa sổ trong `windows`.

    NULL khi cửa sổ THIẾU GIỜ — đó là toàn bộ ngữ nghĩa completeness, không có
    cột `_is_complete` song song. `SUM` trên cửa sổ thiếu giờ vẫn ra số, chỉ là
    số SAI (tổng 18 giờ gán nhãn "24h"), nên phải chặn bằng COUNT.

    `RANGE BETWEEN INTERVAL` chứ không phải `ROWS`: giờ thiếu trong nguồn không
    được phép kéo cửa sổ lùi thêm.
#}
{% macro rolling_rain_sums(windows, partition_by, order_by='valid_time_utc') %}
    {%- for hours in windows %}
    SUM(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
    ) AS rain_{{ hours }}h_sum_raw,
    COUNT(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
    ) AS rain_{{ hours }}h_hours{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{#- Đổi cặp (sum_raw, hours) thành cột công bố; NULL nếu thiếu giờ. -#}
{% macro rolling_rain_columns(windows) %}
    {%- for hours in windows %}
    CASE WHEN rain_{{ hours }}h_hours = {{ hours }}
         THEN rain_{{ hours }}h_sum_raw END AS rain_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{#
    Tổng mưa forecast nhìn về PHÍA TRƯỚC (không tính row hiện tại).

    Archive dùng cửa sổ trailing để mô tả mưa đã rơi. Forecast dùng cửa sổ
    forward để trả lời câu hỏi vận hành: sau giờ valid này đến H giờ tới sẽ có
    bao nhiêu mưa. Hai ngữ nghĩa không dùng chung một window frame.
#}
{% macro forward_rain_sums(windows, partition_by, order_by='valid_time_utc') %}
    {%- for hours in windows %}
    SUM(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN INTERVAL '1 hour' FOLLOWING AND INTERVAL '{{ hours }} hours' FOLLOWING
    ) AS forecast_next_{{ hours }}h_sum_raw,
    COUNT(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN INTERVAL '1 hour' FOLLOWING AND INTERVAL '{{ hours }} hours' FOLLOWING
    ) AS forecast_next_{{ hours }}h_hours{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{% macro forward_rain_columns(windows) %}
    {%- for hours in windows %}
    CASE WHEN forecast_next_{{ hours }}h_hours = {{ hours }}
         THEN forecast_next_{{ hours }}h_sum_raw END
         AS forecast_next_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{#
    Ngưỡng QĐ 2280/QĐ-UBND và quy chuẩn mưa lớn VN.

    Đây là NGƯỠNG PHÁP QUY, không phải tham số tuỳ chỉnh: đổi số ở đây là đổi
    nghĩa của cảnh báo. Giữ một chỗ để còn đối chiếu được với văn bản gốc.
#}
{% macro hanoi_rain_scenario_band(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 100 THEN 'over_100'
        WHEN {{ column }} >= 70 THEN 'from_70_to_100'
        WHEN {{ column }} >= 50 THEN 'from_50_to_under_70'
        ELSE 'below_50'
    END
{% endmacro %}


{% macro vn_rain_band_12h(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 100 THEN 'over_100'
        WHEN {{ column }} >= 70 THEN 'from_70_to_100'
        WHEN {{ column }} >= 50 THEN 'from_50_to_under_70'
        WHEN {{ column }} >= 30 THEN 'from_30_to_under_50'
        ELSE 'below_30'
    END
{% endmacro %}


{% macro vn_rain_band_24h(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 300 THEN 'over_300'
        WHEN {{ column }} > 200 THEN 'from_200_to_300'
        WHEN {{ column }} >= 150 THEN 'from_150_to_under_200'
        WHEN {{ column }} >= 100 THEN 'from_100_to_under_150'
        WHEN {{ column }} >= 50 THEN 'from_50_to_under_100'
        ELSE 'below_50'
    END
{% endmacro %}
