/*
    MART — quan sát ngập đã xác nhận, theo địa điểm × thời điểm × nguồn.

    Grain: `observation_id`. Đây là NHÃN, không phải feature: mọi cột ở đây đến
    từ báo cáo/bài báo, không có cột nào suy ra từ Open-Meteo.

    Bảng này là thứ docs/05 §14 gọi là K5 — nguồn nhãn sự kiện để hiệu chỉnh
    POD/FAR/CSI. Trước khi có nó, dự án không có cách nào biết một ngưỡng mưa là
    đúng hay sai.

    ─── Cảnh báo về thành phần nhãn ────────────────────────────────────────
    Nguồn đầu tiên (bảng VnExpress 07/10/2025) là DANH SÁCH ĐIỂM ĐANG NGẬP:
    122/123 dòng là nhãn dương. Một tập gần như toàn dương đo được RECALL (POD)
    nhưng KHÔNG đo được FAR hay CSI — luật "báo động ở mọi nơi" cũng đạt
    POD = 1,0 trên tập này. Muốn có nhãn âm đáng tin cần danh sách toàn thành
    phố do cơ quan chức năng công bố, không phải bài báo.

    Incremental để một lần sửa nguồn (đính chính độ sâu, hạ hạng nguồn) không
    xoá lịch sử các dòng còn lại.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'observation_id',
    tags = ['fact', 'flood']
) }}

SELECT
    o.observation_id,
    o.event_id,
    {#
        Danh tính ĐỊA ĐIỂM, khác với danh tính quan sát.

        Tần suất ngập lịch sử — biến mạnh nhất khi thiếu dữ liệu hạ tầng — là
        phép đếm trên địa điểm, nên nó cần một khoá bền qua nhiều trận mưa và
        nhiều nguồn.

        Hệ quả phải biết trước: hai bài báo viết tên cùng một chỗ khác nhau sẽ
        thành hai địa điểm và chia đôi tần suất. Chuẩn hoá tên là việc thủ công
        không tránh được, không phải thứ SQL đoán hộ được.
    #}
    MD5(LOWER(TRIM(o.location_name_raw))) AS location_key,
    o.location_name_raw,
    o.ward_code,
    o.observed_at_utc,
    CAST(o.observed_at_utc AS DATE) AS observed_date_utc,
    o.observed_at_precision,
    o.as_of_utc,
    o.is_flooded,
    o.depth_min_cm,
    o.depth_max_cm,
    o.depth_text_raw,
    o.traffic_status,
    o.traffic_text_raw,
    o.source_grade,
    o.source_publisher,
    o.source_url,
    o.source_visualisation_id,
    o.source_visualisation_version,
    o.source_updated_at_utc,
    {#
        Cờ dùng-được-để-huấn-luyện, tách khỏi is_flooded. Cần CẢ BA:

        - hạng A/B/C: hạng D (mạng xã hội chưa xác minh) chưa kiểm chứng được;
        - biết rõ GIỜ: ghép mưa 1h với một nhãn "sáng 7/10" là gán sai thời
          điểm, và sai thời điểm tệ hơn thiếu dòng;
        - có ward_code: không có phường thì không có ô lưới, không có ô lưới
          thì không có lượng mưa để ghép. Hiện TOÀN BỘ quan sát trượt điều kiện
          này vì nguồn ghi địa điểm theo cột mốc đường, chưa geocode.
    #}
    (
        o.source_grade IN ('A', 'B', 'C')
        AND o.observed_at_precision = 'hour'
        AND o.ward_code IS NOT NULL
    ) AS is_training_eligible,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('stg_seed__flood_observation') }} o
