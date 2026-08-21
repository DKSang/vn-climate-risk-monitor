{{ config(materialized = 'view') }}

/*
    SILVER — centroid phường/xã chuẩn hóa, giữ grain 3.321 đơn vị toàn quốc.
*/

SELECT
    CAST(location_key AS INTEGER) AS location_key,
    TRIM(province_code) AS province_code,
    TRIM(province_name) AS province_name,
    TRIM(commune_code) AS ward_code,
    TRIM(commune_name) AS ward_name,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    TRIM(region) AS region,
    TRIM(climate_zone) AS climate_zone,
    _source AS bronze_source,
    _ingested_at AS bronze_ingested_at
FROM {{ ref('ward_coordinates') }}
