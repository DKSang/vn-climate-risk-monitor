/*
    SILVER — 126 phường/xã Hà Nội, lọc từ 3.321 đơn vị toàn quốc.

    MỘT nguồn duy nhất cho thực thể này: seed. Bản cũ còn đọc song song
    `pg_source.public.wards` qua dbt — hai đường cho cùng một danh mục, và khi
    chúng lệch nhau thì không có quy tắc nào nói bên nào đúng.

    `province_code = '01'` là Hà Nội sau sắp xếp 2025 (NQ 1656/NQ-UBTVQH15).
    Số 126 được khoá bằng test, không phải bằng niềm tin.
*/

{{ config(materialized = 'view') }}

SELECT
    commune_code AS ward_code,
    commune_name AS ward_name,
    province_code,
    province_name,
    latitude AS ward_latitude,
    longitude AS ward_longitude,
    region,
    climate_zone
FROM {{ ref('ward_coordinates_seed') }}
WHERE province_code = '01'
