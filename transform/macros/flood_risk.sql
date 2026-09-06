{#
    Điểm rủi ro ngập — BASELINE HEURISTIC, CHƯA ĐƯỢC HIỆU CHỈNH.

    ─── Đọc kỹ trước khi dùng ───────────────────────────────────────────────
    Các trọng số dưới đây do người đặt ra, không do dữ liệu học được. Chúng tồn
    tại để có MỘT đường cơ sở minh bạch đem đi backtest, chứ không phải vì đã
    được chứng minh. Đầu ra KHÔNG phải xác suất ngập và không được đặt tên,
    hiển thị hay diễn giải như xác suất — docs/05 §2.2.

    Khi `fct_flood_training_feature` đã có đủ sự kiện qua nhiều trận mưa, mùa và
    địa bàn, trọng số phải được thay bằng hệ số hồi quy logistic có
    regularization, và version phải tăng.

    ─── Vì sao có số hạng tương tác H×V ────────────────────────────────────
    Cùng một lượng mưa, chỗ thoát nước tốt và điểm ngập lặp lại không thể nhận
    cùng mức cảnh báo. Mô hình cộng thuần tuý không diễn đạt được điều đó: nó
    cho phép mưa rất lớn ở nơi chưa từng ngập bằng điểm với mưa vừa ở điểm đen.
#}

{% macro flood_risk_version() %}
    {{ return('heuristic_baseline_v1_uncalibrated') }}
{% endmacro %}


{#
    Trọng số áp lực khí tượng H. Tổng = 1,0.

    Dồn trọng số vào 6h và 12h vì đó là thang thời gian mà ngập đô thị do mưa
    thực sự hình thành: 1 giờ quá ngắn để lấp đầy hệ thống, 72 giờ chỉ mô tả
    điều kiện tiền kỳ. Trận 07/10/2025 là bằng chứng — đỉnh R1 toàn thành phố
    chỉ 24,8 mm trong khi R12 đạt 140,7 mm.
#}
{% macro flood_hazard_weights() %}
    {{ return({
        'rain_1h_mm':  0.10,
        'rain_3h_mm':  0.15,
        'rain_6h_mm':  0.20,
        'rain_12h_mm': 0.20,
        'rain_24h_mm': 0.15,
        'rain_72h_mm': 0.10,
        'soil_moisture_index': 0.10,
    }) }}
{% endmacro %}


{#
    Trọng số dễ tổn thương V. Tổng = 1,0 nhưng CHƯA đủ biến.

    `impervious_surface_ratio`, `relative_elevation` và độ nhạy lưu vực thoát
    nước không có trong repo (không DEM, không land cover, không sơ đồ cống),
    nên phần trọng số của chúng được dồn vào lịch sử ngập — biến duy nhất đang
    quan sát được. Đó là một GIẢ ĐỊNH, không phải một phép chuẩn hoá vô hại:
    nó ngầm coi lịch sử ngập đại diện đủ cho cả ba biến vắng mặt.
#}
{% macro flood_vulnerability_weights() %}
    {{ return({
        'historical_flood_frequency': 0.60,
        'historical_impassable_rate': 0.25,
        'is_known_flood_point':       0.15,
    }) }}
{% endmacro %}


{#- Tổng có trọng số, bỏ qua thành phần NULL và CHIA LẠI theo trọng số có mặt.

    Chỉ dùng cho H, nơi mọi thành phần đo CÙNG một thứ (áp lực mưa) ở các thang
    thời gian khác nhau. Khi đó thiếu R72 mà có R1..R24 vẫn ước lượng được áp
    lực, và chia lại giữ thang 0–1 mà không bịa dữ liệu.

    KHÔNG dùng cho V. Các thành phần của V đo những thứ khác nhau, nên chia lại
    biến thành phần duy nhất có mặt thành toàn bộ điểm: một phường chỉ vì có tên
    trong danh mục QĐ 2280 sẽ nhận V = 1,0, mức tổn thương cao nhất, dù chưa có
    một quan sát ngập nào. Dùng `weighted_sum` cho V. -#}
{% macro weighted_mean(weights) %}
    (
        ({%- for column, weight in weights.items() %}
        COALESCE({{ column }} * {{ weight }}, 0){{ " +" if not loop.last }}
        {%- endfor %})
        / NULLIF(({%- for column, weight in weights.items() %}
        CASE WHEN {{ column }} IS NULL THEN 0 ELSE {{ weight }} END{{ " +" if not loop.last }}
        {%- endfor %}), 0)
    )
{% endmacro %}


{#- Tổng có trọng số với MẪU SỐ CỐ ĐỊNH = tổng toàn bộ trọng số.

    Thành phần NULL đóng góp 0 và KHÔNG được chia lại. Dùng cho V, nơi các
    thành phần đo những thứ khác nhau và sự vắng mặt của bằng chứng là thông
    tin thật: một phường chưa có quan sát ngập nào thì đúng là chưa có bằng
    chứng về tổn thương, và điểm phải phản ánh điều đó thay vì suy ra từ mảnh
    bằng chứng duy nhất còn lại. -#}
{% macro weighted_sum(weights) %}
    (
        ({%- for column, weight in weights.items() %}
        COALESCE({{ column }} * {{ weight }}, 0){{ " +" if not loop.last }}
        {%- endfor %})
        / {{ weights.values() | sum }}
    )
{% endmacro %}


{#- Chuẩn hoá mưa về 0–1 theo trần p99 của CHÍNH ô lưới và tháng đó.

    Tháng khô có thể có p99 = 0. Khi đó mọi lượng mưa dương đều là cực đoan so
    với lịch sử của chính nó, nên trả 1 — chứ không phải NULL (mất dòng) hay 0
    (nói rằng mưa này bình thường). -#}
{% macro normalize_by_p99(value_column, p99_column) %}
    CASE
        WHEN {{ value_column }} IS NULL THEN NULL
        WHEN COALESCE({{ p99_column }}, 0) = 0
            THEN CASE WHEN {{ value_column }} > 0 THEN 1.0 ELSE 0.0 END
        ELSE LEAST({{ value_column }} / {{ p99_column }}, 1.0)
    END
{% endmacro %}


{#- Chuẩn hoá p10–p90 rồi kẹp về 0–1 (docs/05 §5.2, dùng cho độ ẩm đất). -#}
{% macro normalize_p10_p90(value_column, p10_column, p90_column) %}
    LEAST(GREATEST(
        ({{ value_column }} - {{ p10_column }})
            / NULLIF({{ p90_column }} - {{ p10_column }}, 0),
        0.0), 1.0)
{% endmacro %}
