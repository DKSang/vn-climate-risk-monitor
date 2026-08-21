{{ config(materialized = 'view') }}

/*
    SILVER — Data cleaning and validation (chuẩn Microsoft medallion).

    Làm sạch nguồn toạ độ centroid phường/xã.

    Ghi chú: trước đây model này phải LPAD(CAST(commune_code AS VARCHAR), 5, '0')
    vì read_csv_auto() trên MinIO suy kiểu mã hành chính thành BIGINT và làm mất
    số 0 đầu. Sau khi chuyển sang dbt seed với column_types ép VARCHAR, bronze đã
    giữ đúng '01' / '00004' như CSV gốc -> bỏ được hack đó, chỉ còn TRIM.

    Giữ nguyên grain nguồn — 3.321 phường/xã toàn quốc, KHÔNG lọc tỉnh.
    Dedup: commune_code và location_key đều duy nhất 3.321/3.321 (đã kiểm chứng).
*/

SELECT
    CAST(location_key AS INTEGER)   AS location_key,
    TRIM(province_code)             AS province_code,
    TRIM(province_name)             AS province_name,
    TRIM(commune_code)              AS ward_code,
    TRIM(commune_name)              AS ward_name,
    CAST(latitude AS DOUBLE)        AS latitude,
    CAST(longitude AS DOUBLE)       AS longitude,
    TRIM(region)                    AS region,
    TRIM(climate_zone)              AS climate_zone,
    _source                         AS bronze_source,
    _ingested_at                    AS bronze_ingested_at
FROM {{ ref('ward_coordinates_raw') }}
