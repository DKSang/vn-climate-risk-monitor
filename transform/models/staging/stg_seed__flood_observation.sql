/*
    STAGING — quan sát ngập thực tế từ seed Flourish/VnExpress.

    Một dòng = MỘT quan sát tại một địa điểm, một thời điểm, từ MỘT nguồn. Không
    phải danh mục điểm ngập (`stg_seed__flood_point` mới là danh mục): danh mục
    nói chỗ nào CÓ THỂ ngập, bảng này nói chỗ nào ĐÃ ngập lúc nào.

    Giữ nguyên ba cột `*_raw` bên cạnh cột đã chuẩn hoá. Đây là dữ liệu điền từ
    câu chữ tiếng Việt của báo, nên mọi phép suy diễn (10 cm, "lưu thông chậm",
    "lúc 6h30") phải đối chiếu lại được với nguyên văn khi nghi ngờ.

    ─── ward_code CÓ THỂ NULL, và đó là vấn đề chưa giải ───────────────────
    Nguồn ghi địa điểm theo đoạn đường và cột mốc ("ĐLTL đoạn Km 8+200",
    "Đường QL32: Km14+500"), không ghi phường. Không có phép khớp chuỗi nào
    biến chúng thành một trong 126 phường — việc đó cần geocoding thật.
    Không đoán bừa: quan sát chưa gắn được phường sẽ ở lại bảng nhãn nhưng
    KHÔNG vào tập huấn luyện, vì không có phường thì không có ô lưới, và không
    có ô lưới thì không có lượng mưa để ghép.
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
)

SELECT
    observation_id,
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
    -- Chưa gắn được phường từ mô tả đoạn đường; chờ geocoding.
    CAST(NULL AS VARCHAR) AS ward_code
FROM seed_data
