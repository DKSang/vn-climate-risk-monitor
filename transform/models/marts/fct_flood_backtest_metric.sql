/*
    MART — POD / FAR / CSI theo từng luật cảnh báo, tách theo TRẬN MƯA.

    Grain: (rule_name, threshold_value, event_id).

    ─── Vì sao tách theo event_id chứ không chia ngẫu nhiên từng dòng ───────
    Các quan sát trong cùng một trận mưa có quan hệ với nhau: cùng ô lưới, cùng
    hệ thống thời tiết, thường cùng nguồn báo. Chia ngẫu nhiên sẽ để cùng một
    trận nằm ở cả train lẫn test và cho ra chỉ số đẹp giả. Giữ nguyên một trận
    làm holdout là cách duy nhất đọc được con số — docs/05 §10.

    ─── Ba luật được đo cạnh nhau ──────────────────────────────────────────
    `hanoi_1h_band`  luật ĐANG CHẠY: rain_1h vượt 50/70/100 theo QĐ 2280
    `vn_24h_band`    tích luỹ 24 giờ vượt dải QĐ 18
    `risk_score`     heuristic chưa hiệu chỉnh của flood_risk.sql
    Điểm mới chỉ đáng thay thế nếu nó THẮNG luật đang chạy trên cùng tập nhãn.
    Không so với baseline thì một CSI = 0,6 chẳng nói lên điều gì.

    ─── Giới hạn phải đọc cùng con số ──────────────────────────────────────
    FAR ở đây tính TRÊN TẬP ĐÃ QUAN SÁT, không phải trên toàn thành phố. Địa
    điểm không xuất hiện trong nguồn là `unknown`, không phải "không ngập", nên
    không được đưa vào mẫu số. Con số này vì vậy là FAR có điều kiện trên tập
    quan sát và sẽ LẠC QUAN hơn FAR thật.
*/

{{ config(
    materialized = 'table',
    tags = ['fact', 'flood']
) }}

{% set hazard_weights = flood_hazard_weights() %}
{% set vulnerability_weights = flood_vulnerability_weights() %}

WITH scored AS (
    SELECT
        f.*,
        {% set hazard_inputs = {} %}
        {%- for column, weight in hazard_weights.items() %}
        {%- if column.startswith('rain_') %}
        {%- do hazard_inputs.update({column.replace('_mm', '_normalized'): weight}) %}
        {%- else %}
        {%- do hazard_inputs.update({column: weight}) %}
        {%- endif %}
        {%- endfor %}
        {{ weighted_mean(hazard_inputs) }} AS hazard_index,
        {{ weighted_sum(vulnerability_weights) }} AS vulnerability_index
    FROM {{ ref('fct_flood_training_feature') }} f
),

predicted AS (
    SELECT
        *,
        100.0 / (1.0 + EXP(-(
            -3.0
            + 3.0 * COALESCE(hazard_index, 0)
            + 2.0 * COALESCE(vulnerability_index, 0)
            + 2.0 * COALESCE(hazard_index, 0) * COALESCE(vulnerability_index, 0)
        ))) AS risk_score
    FROM scored
),

thresholds AS (
    SELECT UNNEST(GENERATE_SERIES(5, 95, 5)) AS threshold_value
),

rule_outcome AS (
    -- Luật đang chạy và luật 24 giờ không có tham số, nên gắn threshold NULL:
    -- gán một số giả sẽ khiến chúng trông như có thể tinh chỉnh.
    SELECT
        'hanoi_1h_band' AS rule_name,
        CAST(NULL AS INTEGER) AS threshold_value,
        event_id,
        is_flooded,
        hanoi_rain_scenario_band <> 'below_50' AS predicted_flood,
        CAST(NULL AS DOUBLE) AS predicted_probability
    FROM predicted

    UNION ALL

    SELECT
        'vn_24h_band',
        NULL,
        event_id,
        is_flooded,
        vn_rain_band_24h NOT IN ('below_50', 'from_50_to_under_100'),
        NULL
    FROM predicted

    UNION ALL

    SELECT
        'risk_score',
        t.threshold_value,
        p.event_id,
        p.is_flooded,
        p.risk_score >= t.threshold_value,
        p.risk_score / 100.0
    FROM predicted p
    CROSS JOIN thresholds t
)

SELECT
    rule_name,
    threshold_value,
    event_id,
    COUNT(*) AS observation_count,
    COUNT(*) FILTER (WHERE is_flooded) AS positive_observation_count,
    COUNT(*) FILTER (WHERE NOT is_flooded) AS negative_observation_count,
    COUNT(*) FILTER (WHERE is_flooded AND predicted_flood) AS hit_count,
    COUNT(*) FILTER (WHERE is_flooded AND NOT predicted_flood) AS miss_count,
    COUNT(*) FILTER (WHERE NOT is_flooded AND predicted_flood) AS false_alarm_count,
    COUNT(*) FILTER (WHERE NOT is_flooded AND NOT predicted_flood)
        AS correct_negative_count,

    -- POD = H / (H + M). NULL khi trận không có nhãn dương nào — chia cho 0 ở
    -- đây là "không đo được", không phải "bằng 0".
    CAST(COUNT(*) FILTER (WHERE is_flooded AND predicted_flood) AS DOUBLE)
        / NULLIF(COUNT(*) FILTER (WHERE is_flooded), 0) AS pod,

    -- FAR chỉ được công bố khi tập đánh giá có nhãn âm. Tập toàn dương trả
    -- NULL thay vì 0: không quan sát được false alarm không có nghĩa là mô
    -- hình không tạo false alarm ngoài tập bài báo.
    CASE WHEN COUNT(*) FILTER (WHERE NOT is_flooded) > 0
        THEN CAST(
            COUNT(*) FILTER (WHERE NOT is_flooded AND predicted_flood) AS DOUBLE
        ) / NULLIF(COUNT(*) FILTER (WHERE predicted_flood), 0)
    END AS far,

    -- CSI = H / (H + M + F).
    CASE WHEN COUNT(*) FILTER (WHERE is_flooded) > 0
               AND COUNT(*) FILTER (WHERE NOT is_flooded) > 0
        THEN CAST(
            COUNT(*) FILTER (WHERE is_flooded AND predicted_flood) AS DOUBLE
        ) / NULLIF(COUNT(*) FILTER (WHERE is_flooded OR predicted_flood), 0)
    END AS csi,

    -- Brier chỉ có nghĩa với luật cho ra số liên tục, và chỉ khi số đó đã được
    -- hiệu chỉnh thành xác suất. Ở version hiện tại nó CHƯA — giữ cột để theo
    -- dõi thay đổi giữa các version, không để công bố.
    CASE WHEN COUNT(*) FILTER (WHERE is_flooded) > 0
               AND COUNT(*) FILTER (WHERE NOT is_flooded) > 0
        THEN AVG(POWER(
            predicted_probability - CASE WHEN is_flooded THEN 1.0 ELSE 0.0 END, 2
        ))
    END AS brier_score,

    '{{ flood_risk_version() }}' AS risk_model_version,
    {{ processing_updated_at() }} AS _updated_at
FROM rule_outcome
GROUP BY rule_name, threshold_value, event_id
