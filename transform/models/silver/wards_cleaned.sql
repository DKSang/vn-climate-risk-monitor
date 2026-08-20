{{ config(materialized = 'view') }}

/*
    SILVER — Data cleaning and validation (chuẩn Microsoft medallion).

    Thao tác lớp silver theo tài liệu Microsoft:
      schema enforcement · null/missing values · deduplication ·
      type casting · joins · schema evolution

    Model này làm phần "cleaning" cho một nguồn: danh mục phường/xã GSO.
    Giữ nguyên grain nguồn — 3.321 phường/xã toàn quốc, KHÔNG lọc tỉnh
    (Microsoft: silver phải có "at least one validated, non-aggregated
    representation of each record").

    Dedup: đã kiểm chứng 2026-08-20, `code` duy nhất 3.321/3.321 -> không thêm
    logic dedup thừa; test unique ở schema.yml sẽ bắt nếu nguồn đổi.
*/

SELECT
    TRIM(code)                                  AS ward_code,
    TRIM(name)                                  AS ward_name,
    TRIM(name_en)                               AS ward_name_en,
    TRIM(full_name)                             AS ward_full_name,
    TRIM(full_name_en)                          AS ward_full_name_en,
    TRIM(code_name)                             AS ward_slug,
    LPAD(TRIM(province_code), 2, '0')           AS province_code,
    CAST(administrative_unit_id AS INTEGER)     AS administrative_unit_id,
    _source                                     AS bronze_source,
    _ingested_at                                AS bronze_ingested_at
FROM {{ ref('wards_raw') }}
