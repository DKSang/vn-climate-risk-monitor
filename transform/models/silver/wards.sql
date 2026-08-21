{{ config(materialized = 'view') }}

/*
    SILVER — danh mục phường/xã chuẩn hóa, chưa aggregate và chưa lọc tỉnh.
*/

SELECT
    TRIM(code) AS ward_code,
    TRIM(name) AS ward_name,
    TRIM(name_en) AS ward_name_en,
    TRIM(full_name) AS ward_full_name,
    TRIM(full_name_en) AS ward_full_name_en,
    TRIM(code_name) AS ward_slug,
    LPAD(TRIM(province_code), 2, '0') AS province_code,
    CAST(administrative_unit_id AS INTEGER) AS administrative_unit_id,
    _source AS bronze_source,
    _ingested_at AS bronze_ingested_at
FROM {{ ref('gso_wards') }}
