{{ config(materialized = 'table') }}

/*
    BRONZE — Raw data ingestion (chuẩn Microsoft medallion).

    Toạ độ centroid 3.321 phường/xã, đọc thẳng CSV trên MinIO bằng read_csv_auto().

    CẤM: ép kiểu, đổi tên, lọc, JOIN, dedup.
    ĐƯỢC: thêm cột provenance.
*/

SELECT
    *,
    's3://vn-climate/raw/geography/crawl/coordinates_locations/full/2026/08/20/dim_location.csv' AS _source,
    CURRENT_TIMESTAMP AS _ingested_at
FROM {{ source('raw_files', 'dim_location') }}
