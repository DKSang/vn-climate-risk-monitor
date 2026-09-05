/*
    MART — cầu nối phường ↔ ô lưới, theo từng model.

    252 dòng. Nhỏ đến mức mọi câu hỏi theo GIỜ ở cấp phường chỉ cần join bảng
    này với `fct_rain_archive_hourly` lúc query — không cần một fact phường×giờ nhân bản
    26 triệu dòng giống hệt nhau (126 phường chỉ có 12 hoặc 48 giá trị khác
    nhau ở mỗi giờ).

    `ward_count_on_grid` để xếp hạng áp lực mưa mà KHÔNG nhân bản trọng số khi
    nhiều phường dùng chung một ô (Q4).
*/

{#
    INCREMENTAL cùng lý do với `dim_ward`: giữ dòng của phường đã giải thể để
    `fct_ward_rain_archive_daily` không mất lịch sử khi INNER JOIN.
#}
{{ config(
    materialized = 'incremental',
    unique_key = 'ward_grid_key',
    tags = ['bridge']
) }}

SELECT
    MD5(CONCAT_WS('|', map.ward_code, map.weather_model)) AS ward_grid_key,
    map.ward_code,
    map.weather_model,
    map.grid_cell_id,
    map.grid_elevation_m,
    COUNT(*) OVER (PARTITION BY map.weather_model, map.grid_cell_id)
        AS ward_count_on_grid,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    {{ processing_updated_at() }} AS _updated_at
FROM {{ ref('stg_seed__ward_grid') }} AS map
-- INNER JOIN: một dòng ánh xạ trỏ tới ô không tồn tại trong dữ liệu là lỗi
-- ánh xạ, và test relationships sẽ chỉ ra ngay thay vì để nó lặng lẽ sinh
-- fact phường rỗng.
INNER JOIN {{ ref('dim_grid') }} AS grid
    ON grid.grid_cell_id = map.grid_cell_id
