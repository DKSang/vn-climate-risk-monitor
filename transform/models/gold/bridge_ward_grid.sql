/*
    GOLD — cầu nối phường ↔ ô lưới, theo từng model.

    252 dòng. Nhỏ đến mức mọi câu hỏi theo GIỜ ở cấp phường chỉ cần join bảng
    này với `fct_rain_hourly` lúc query — không cần một fact phường×giờ nhân bản
    26 triệu dòng giống hệt nhau (126 phường chỉ có 12 hoặc 48 giá trị khác
    nhau ở mỗi giờ).

    `ward_count_on_grid` để xếp hạng áp lực mưa mà KHÔNG nhân bản trọng số khi
    nhiều phường dùng chung một ô (Q4).
*/

{{ config(
    materialized = 'table',
    tags = ['gold', 'bridge']
) }}

SELECT
    map.ward_code,
    map.weather_model,
    map.grid_cell_id,
    map.grid_elevation_m,
    COUNT(*) OVER (PARTITION BY map.weather_model, map.grid_cell_id)
        AS ward_count_on_grid
FROM {{ ref('ward_grid') }} AS map
-- INNER JOIN: một dòng ánh xạ trỏ tới ô không tồn tại trong dữ liệu là lỗi
-- ánh xạ, và test relationships sẽ chỉ ra ngay thay vì để nó lặng lẽ sinh
-- fact phường rỗng.
INNER JOIN {{ ref('dim_grid') }} AS grid
    ON grid.grid_cell_id = map.grid_cell_id
