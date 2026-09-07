/*
    STAGING — quan sát ngập thực tế từ seed Flourish/VnExpress.

    Một dòng = MỘT quan sát tại một địa điểm, một thời điểm, từ MỘT nguồn. Không
    phải danh mục điểm ngập (`stg_seed__flood_point` mới là danh mục): danh mục
    nói chỗ nào CÓ THỂ ngập, bảng này nói chỗ nào ĐÃ ngập lúc nào.

    Giữ nguyên ba cột `*_raw` bên cạnh cột đã chuẩn hoá. Đây là dữ liệu điền từ
    câu chữ tiếng Việt của báo, nên mọi phép suy diễn (10 cm, "lưu thông chậm",
    "lúc 6h30") phải đối chiếu lại được với nguyên văn khi nghi ngờ.

    ─── ward_code đến từ seed geocode RIÊNG, không từ nguồn ────────────────
    Nguồn ghi địa điểm theo đoạn đường và cột mốc ("ĐLTL đoạn Km 8+200",
    "Đường QL32: Km14+500"), không ghi phường. Toạ độ và phường nằm ở
    `flood_observation_geocode_seed` — máy geocode nháp, người soát.

    Chỉ nhận `ward_code` của dòng đã `geocode_verified = true`. Dòng chưa soát
    vẫn giữ toạ độ để hiển thị nhưng ward_code là NULL, nên không vào tập huấn
    luyện. Không đoán bừa: không có phường thì không có ô lưới, không có ô lưới
    thì không có lượng mưa để ghép, và một phường SAI còn tệ hơn không có
    phường — nó ghép nhãn với lượng mưa của chỗ khác.
*/

{{ config(materialized = 'view') }}

WITH seed_data AS (
    SELECT
        observation_id,
        event_id,
        location_name_raw,
        depth_text_raw,
        traffic_text_raw,
        depth_min_cm,
        depth_max_cm,
        traffic_status,
        NULLIF(TRIM(observed_at_local), '') AS observed_at_local,
        NULLIF(TRIM(as_of_local), '') AS as_of_local,
        is_flooded,
        source_grade,
        source_publisher,
        source_url,
        source_visualisation_id,
        source_visualisation_version,
        source_updated_at_utc
    FROM {{ ref('flood_event_observations_seed') }}
),

geocode AS (
    SELECT
        observation_id,
        latitude,
        longitude,
        ward_code,
        geocode_match_type,
        geocode_verified,
        anchor_type,
        confidence AS geocode_confidence,
        coordinate_basis,
        needs_manual_validation,
        NULLIF(TRIM(verified_at_utc), '') AS verified_at_utc,
        NULLIF(TRIM(verification_method), '') AS verification_method,
        review_note
    FROM {{ ref('flood_observation_geocode_seed') }}
)

SELECT
    s.observation_id,
    event_id,
    location_name_raw,
    {#
        Seed đã ghi kèm offset `+07:00`, nên CAST thẳng sang TIMESTAMPTZ là
        đúng và không cần `AT TIME ZONE`. Dùng thêm `AT TIME ZONE` ở đây sẽ
        diễn giải lại một mốc vốn đã tuyệt đối và làm lệch 7 giờ.
    #}
    CAST(observed_at_local AS TIMESTAMPTZ) AS observed_at_utc,
    observed_at_local,
    -- Nguồn hoặc ghi rõ "lúc 6h30", hoặc không ghi gì. Không có mức trung gian.
    CASE WHEN observed_at_local IS NULL THEN 'unknown' ELSE 'hour' END
        AS observed_at_precision,
    CAST(as_of_local AS TIMESTAMPTZ) AS as_of_utc,
    is_flooded,
    depth_min_cm,
    depth_max_cm,
    depth_text_raw,
    traffic_status,
    traffic_text_raw,
    source_grade,
    source_publisher,
    source_url,
    source_visualisation_id,
    source_visualisation_version,
    CAST(source_updated_at_utc AS TIMESTAMPTZ) AS source_updated_at_utc,
    g.latitude,
    g.longitude,
    g.geocode_match_type,
    g.anchor_type,
    g.geocode_confidence,
    g.coordinate_basis,
    COALESCE(g.needs_manual_validation, TRUE) AS needs_manual_validation,
    CAST(g.verified_at_utc AS TIMESTAMPTZ) AS geocode_verified_at_utc,
    g.verification_method,
    g.review_note AS geocode_review_note,
    COALESCE(g.geocode_verified, FALSE) AS geocode_verified,
    -- CHỈ nhận phường đã được người soát. Toạ độ chưa soát vẫn giữ để hiển thị.
    CASE WHEN g.geocode_verified THEN g.ward_code END AS ward_code
FROM seed_data s
LEFT JOIN geocode g USING (observation_id)
