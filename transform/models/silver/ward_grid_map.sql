{{ config(materialized = 'view') }}

/*
    SILVER — phường Hà Nội ↔ ô lưới model.

    Thay cho phép chiếu nearest-neighbour tự tính: ánh xạ ở đây là ô mà Open-Meteo
    THẬT SỰ trả về cho toạ độ phường (seed sinh bởi `make map-grid`). Seed hiện có
    12 ô era5 / 48 ô ecmwf_ifs; bin 0,25° tự tính từng lệch so với snap của API.

    Đây là mảnh ghép cho phép Bronze fetch theo ô: dữ liệu về theo ô, phường được
    gắn lại ở đây, kết quả giống hệt fetch theo phường với 1/9,7 chi phí quota.
*/

SELECT
    seed.model                       AS weather_model,
    'archive'                        AS weather_product,
    seed.ward_code,
    ward.ward_key,
    ward.ward_name,
    seed.ward_latitude               AS requested_latitude,
    seed.ward_longitude              AS requested_longitude,
    ROUND(seed.grid_latitude, 6)    AS grid_latitude,
    ROUND(seed.grid_longitude, 6)    AS grid_longitude,
    {{ grid_cell_id(
        'seed.model', "'archive'",
        'ROUND(seed.grid_latitude, 6)', 'ROUND(seed.grid_longitude, 6)'
    ) }}                             AS grid_cell_id,
    seed.elevation_m                 AS grid_elevation_m,
    -- Khoảng cách phường → tâm ô: dùng để cảnh báo khi ánh xạ quá xa, và để
    -- Gold biết mức "thô" của tín hiệu cho từng phường.
    111.32 * SQRT(
        POWER(seed.ward_latitude - ROUND(seed.grid_latitude, 6), 2)
        + POWER(
            COS(RADIANS(seed.ward_latitude))
            * (seed.ward_longitude - ROUND(seed.grid_longitude, 6)),
            2
        )
    )                                AS mapping_distance_km
FROM {{ ref('ward_grid_map_seed') }} AS seed
INNER JOIN {{ ref('dim_hanoi_ward') }} AS ward
    ON ward.ward_code = seed.ward_code
