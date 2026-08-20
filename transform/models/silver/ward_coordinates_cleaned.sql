{{ config(materialized = 'view') }}

/*
    SILVER — Data cleaning and validation (chuẩn Microsoft medallion).

    Làm sạch nguồn toạ độ centroid phường/xã.

    Type casting (thao tác silver theo Microsoft): ở bronze, province_code và
    commune_code là BIGINT (1, 4) trong khi danh mục GSO lưu VARCHAR đã zero-pad
    ('01', '00004'). Ép về cùng dạng ở đây để join được ở ward_locations.

    Giữ nguyên grain nguồn — 3.321 phường/xã toàn quốc, KHÔNG lọc tỉnh.
    Dedup: commune_code và location_key đều duy nhất 3.321/3.321 (đã kiểm chứng).
*/

SELECT
    CAST(location_key AS INTEGER)                  AS location_key,
    LPAD(CAST(province_code AS VARCHAR), 2, '0')   AS province_code,
    TRIM(province_name)                            AS province_name,
    LPAD(CAST(commune_code AS VARCHAR), 5, '0')    AS ward_code,
    TRIM(commune_name)                             AS ward_name,
    CAST(latitude AS DOUBLE)                       AS latitude,
    CAST(longitude AS DOUBLE)                      AS longitude,
    TRIM(region)                                   AS region,
    TRIM(climate_zone)                             AS climate_zone,
    _source                                        AS bronze_source,
    _ingested_at                                   AS bronze_ingested_at
FROM {{ ref('ward_coordinates_raw') }}
