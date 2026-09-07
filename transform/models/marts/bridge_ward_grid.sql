/*
    MART — cầu nối phường ↔ ô lưới, theo từng model.

    378 dòng ở snapshot hiện tại (archive + forecast). Nhỏ đến mức mọi câu hỏi
    theo GIỜ ở cấp phường chỉ cần join bảng
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

WITH archive_map AS (
    SELECT
        ward_code,
        weather_model,
        grid_cell_id,
        grid_elevation_m
    FROM {{ ref('stg_seed__ward_grid') }}
),

-- Forecast best_match có thể đổi mesh. Lấy tập ô của horizon hiện hành rồi
-- ánh xạ centroid phường tới tâm ô gần nhất; với snapshot hiện tại kết quả
-- khớp đủ 126/126 với phép snap API đã lưu cho IFS.
current_forecast_grids AS (
    SELECT DISTINCT
        grid_cell_id,
        grid_latitude,
        grid_longitude
    FROM {{ ref('int_weather_forecast_hourly') }}
    WHERE valid_time_utc = (
        SELECT MAX(valid_time_utc)
        FROM {{ ref('int_weather_forecast_hourly') }}
    )
),

forecast_ranked AS (
    SELECT
        ward.ward_code,
        'ecmwf_ifs_fc' AS weather_model,
        grid.grid_cell_id,
        CAST(NULL AS DOUBLE) AS grid_elevation_m,
        ROW_NUMBER() OVER (
            PARTITION BY ward.ward_code
            ORDER BY
                POWER(ward.ward_latitude - grid.grid_latitude, 2)
                + POWER(
                    (ward.ward_longitude - grid.grid_longitude)
                    * COS(RADIANS(ward.ward_latitude)),
                    2
                ),
                grid.grid_cell_id
        ) AS proximity_rank
    FROM {{ ref('stg_seed__ward') }} AS ward
    CROSS JOIN current_forecast_grids AS grid
),

forecast_map AS (
    SELECT
        ward_code,
        weather_model,
        grid_cell_id,
        grid_elevation_m
    FROM forecast_ranked
    WHERE proximity_rank = 1
),

all_maps AS (
    SELECT * FROM archive_map
    UNION ALL
    SELECT * FROM forecast_map
)

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
FROM all_maps AS map
-- INNER JOIN: một dòng ánh xạ trỏ tới ô không tồn tại trong dữ liệu là lỗi
-- ánh xạ, và test relationships sẽ chỉ ra ngay thay vì để nó lặng lẽ sinh
-- fact phường rỗng.
INNER JOIN {{ ref('dim_grid') }} AS grid
    ON grid.grid_cell_id = map.grid_cell_id
