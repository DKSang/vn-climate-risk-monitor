/*
    MART — điểm rủi ro ngập theo phường × giờ trên forecast horizon hiện hành.

    Grain: (ward_code, valid_time_utc).

    ─── KHÔNG PHẢI XÁC SUẤT NGẬP ───────────────────────────────────────────
    `risk_score` là một tổ hợp có trọng số DO NGƯỜI ĐẶT, chưa hiệu chỉnh với
    một sự kiện ngập nào. Nó nằm trên thang 0–100 để so sánh giữa các phường
    trong CÙNG một giờ, không phải để đọc như phần trăm khả năng ngập.
    docs/05 §2.2 vẫn cấm phát hành `flood_probability`; bảng này chỉ được dùng
    cho backtest nội bộ cho tới khi POD/FAR/CSI được đo trên nhãn thật.

    ─── Ba lớp, tách bạch cố ý ─────────────────────────────────────────────
    hazard_index       — hoàn toàn từ Open-Meteo, đo được, không giả định
    vulnerability_index— từ lịch sử ngập quan sát được, rỗng cho tới khi có nhãn
    risk_score         — tổ hợp hai lớp trên, phần giả định nằm hết ở đây
    Giữ ba cột riêng để lớp phục vụ có thể hiển thị áp lực mưa mà không buộc
    phải hiển thị điểm tổng hợp.
*/

{{ config(
    materialized = 'table',
    tags = ['fact', 'flood']
) }}

{% set windows = rain_windows() %}
{% set hazard_weights = flood_hazard_weights() %}
{% set vulnerability_weights = flood_vulnerability_weights() %}

WITH ward_forecast AS (
    /*
        CHỈ horizon hiện hành.

        View current tách khỏi bảng history để risk score không trộn
        nhiều forecast run cho cùng valid_time.
    */
    SELECT
        bwg.ward_code,
        f.grid_cell_id,
        f.valid_time_utc,
        {%- for hours in windows %}
        f.rain_{{ hours }}h_mm,
        {%- endfor %}
        f.hanoi_rain_scenario_band,
        f.vn_rain_band_12h,
        f.vn_rain_band_24h
    FROM {{ ref('fct_rain_forecast_current_hourly') }} f
    JOIN {{ ref('bridge_ward_grid') }} bwg
        ON bwg.grid_cell_id = f.grid_cell_id
       AND bwg.weather_model = 'ecmwf_ifs_fc'
       AND bwg.is_active = TRUE
    WHERE f.valid_time_utc >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
),

normalized AS (
    /*
        Chuẩn hoá theo phân phối của CHÍNH ô lưới và tháng.

        Climatology dựng trên archive (ecmwf_ifs), còn forecast là
        ecmwf_ifs_fc — hai model khác nhau và ô lưới không trùng, nên phải nối
        qua ô lưới ARCHIVE của cùng phường. Đây là một xấp xỉ có ý thức: dùng
        phân phối lịch sử của vị trí đó làm thang đo cho dự báo tại vị trí đó.
        Không có cách nào tốt hơn khi forecast chưa có chuỗi lịch sử riêng.
    */
    SELECT
        wf.ward_code,
        wf.grid_cell_id,
        wf.valid_time_utc,
        {%- for hours in windows %}
        wf.rain_{{ hours }}h_mm,
        {{ normalize_by_p99(
            'wf.rain_' ~ hours ~ 'h_mm',
            'clim.rain_' ~ hours ~ 'h_p99_mm'
        ) }} AS rain_{{ hours }}h_normalized,
        {%- endfor %}
        wf.hanoi_rain_scenario_band,
        wf.vn_rain_band_12h,
        wf.vn_rain_band_24h
    FROM ward_forecast wf
    LEFT JOIN {{ ref('bridge_ward_grid') }} archive_grid
        ON archive_grid.ward_code = wf.ward_code
       AND archive_grid.weather_model = 'ecmwf_ifs'
       AND archive_grid.is_active = TRUE
    LEFT JOIN {{ ref('fct_rain_climatology') }} clim
        ON clim.grid_cell_id = archive_grid.grid_cell_id
       AND clim.calendar_month = MONTH(wf.valid_time_utc)
),

ward_vulnerability AS (
    /*
        Lịch sử ngập gộp về cấp PHƯỜNG.

        Bảng này rỗng cho tới khi `flood_observation_seed` có dòng. Rỗng là
        trạng thái đúng, không phải lỗi: nó làm `vulnerability_index` NULL và
        `risk_score` rơi về đúng phần áp lực mưa đo được — tức hệ thống nói
        "tôi chưa biết gì về chỗ này", thay vì giả vờ biết.
    */
    SELECT
        o.ward_code,
        COUNT(DISTINCT o.event_id) AS observed_event_count,
        AVG(CASE WHEN o.is_flooded THEN 1.0 ELSE 0.0 END)
            AS historical_flood_frequency,
        AVG(CASE WHEN o.traffic_status = 'impassable' THEN 1.0 ELSE 0.0 END)
            FILTER (WHERE o.is_flooded) AS historical_impassable_rate
    FROM {{ ref('fct_flood_event_observation') }} o
    WHERE o.is_active = TRUE
    GROUP BY o.ward_code
),

known_point AS (
    SELECT
        ward_code,
        COUNT(*) AS catalogue_point_count
    FROM {{ ref('dim_flood_point') }}
    WHERE is_active = TRUE AND status = 'active'
    GROUP BY ward_code
),

indexed AS (
    SELECT
        n.*,
        v.observed_event_count,
        v.historical_flood_frequency,
        v.historical_impassable_rate,
        COALESCE(kp.catalogue_point_count, 0) AS catalogue_point_count,
        -- Có tên trong danh mục QĐ 2280 là bằng chứng độc lập với quan sát của
        -- dự án, nên vẫn dùng được khi bảng quan sát còn rỗng.
        CASE WHEN kp.catalogue_point_count > 0 THEN 1.0 ELSE 0.0 END
            AS is_known_flood_point
    FROM normalized n
    LEFT JOIN ward_vulnerability v USING (ward_code)
    LEFT JOIN known_point kp USING (ward_code)
),

scored AS (
    SELECT
        *,
        {% set hazard_inputs = {} %}
        {%- for column, weight in hazard_weights.items() %}
        {%- if column.startswith('rain_') %}
        {%- do hazard_inputs.update({column.replace('_mm', '_normalized'): weight}) %}
        {%- endif %}
        {%- endfor %}
        {#
            Độ ẩm đất không có trên forecast fact (chỉ archive ingest soil
            moisture), nên H ở đây rút gọn còn phần mưa và được chia lại theo
            trọng số CÓ MẶT. Bỏ qua khác với gán 0: gán 0 sẽ hạ điểm mọi phường
            như nhau và làm thang không so được với bản tính trên archive.
        #}
        {{ weighted_mean(hazard_inputs) }} AS hazard_index,
        {#
            `weighted_sum`, KHÔNG phải `weighted_mean`: chia lại theo trọng số
            có mặt sẽ biến `is_known_flood_point` — thành phần duy nhất luôn
            non-NULL khi bảng quan sát còn rỗng — thành toàn bộ điểm V, cho mọi
            phường có tên trong danh mục QĐ 2280 mức tổn thương tối đa 1,0.
        #}
        {{ weighted_sum(vulnerability_weights) }} AS vulnerability_index
    FROM indexed
)

SELECT
    MD5(CONCAT_WS('|', ward_code, CAST(valid_time_utc AS VARCHAR)))
        AS flood_risk_score_key,
    ward_code,
    grid_cell_id,
    valid_time_utc,
    {%- for hours in windows %}
    rain_{{ hours }}h_mm,
    {%- endfor %}
    hazard_index,
    vulnerability_index,
    observed_event_count,
    catalogue_point_count,
    /*
        RiskScore = 100 · σ(−3 + 3H + 2V + 2HV)

        Hằng số −3 đặt điểm nền ở mức thấp khi H và V đều bằng 0 (σ(−3) ≈ 0,047)
        thay vì ở 0,5. Số hạng H×V là chỗ diễn đạt rằng cùng lượng mưa nhưng
        điểm đen và chỗ thoát nước tốt không thể cùng mức cảnh báo.

        V NULL (chưa có quan sát nào) được coi là 0 ở ĐÂY, khác với bên trong
        weighted_mean: ở đây "chưa biết gì về địa điểm" phải kéo điểm về phần
        áp lực mưa thuần tuý, chứ không được làm cả dòng thành NULL.
    */
    ROUND(100.0 / (1.0 + EXP(-(
        -3.0
        + 3.0 * COALESCE(hazard_index, 0)
        + 2.0 * COALESCE(vulnerability_index, 0)
        + 2.0 * COALESCE(hazard_index, 0) * COALESCE(vulnerability_index, 0)
    ))), 2) AS risk_score,
    hanoi_rain_scenario_band,
    vn_rain_band_12h,
    vn_rain_band_24h,
    '{{ flood_risk_version() }}' AS risk_model_version,
    {{ processing_updated_at() }} AS _updated_at
FROM scored
