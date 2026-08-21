{{ config(materialized = 'table') }}

/*
    BRONZE — Raw data ingestion (chuẩn Microsoft medallion).

    Toạ độ centroid 3.321 phường/xã, nguồn là dbt seed
    `transform/seeds/ward_coordinates_seed.csv` (version-control cùng code).

    Trước đây đọc file này từ MinIO bằng read_csv_auto(). Đổi sang seed vì:
      - File tĩnh, chỉ đổi khi có nghị quyết hành chính mới -> đúng loại dữ liệu seed
      - Bỏ được phụ thuộc MinIO cho một file cấu hình
      - Version-control trong git: biết ai đổi gì, khi nào
      - column_types ép VARCHAR giữ được mã zero-pad ('01', '00004') mà
        read_csv_auto làm mất do suy kiểu thành BIGINT

    CẤM: ép kiểu, đổi tên, lọc, JOIN, dedup.
    ĐƯỢC: thêm cột provenance.
*/

SELECT
    *,
    'dbt seed: ward_coordinates_seed.csv'  AS _source,
    CURRENT_TIMESTAMP                      AS _ingested_at
FROM {{ ref('ward_coordinates_seed') }}
