{{ config(materialized = 'table') }}

/*
    BRONZE — Raw data ingestion (chuẩn Microsoft medallion).

    Nhiệm vụ lớp này theo tài liệu Microsoft:
      "Store everything exactly as it arrives. No changes are allowed."
      "Contains and maintains the raw state of the data source in its original formats."
      "Serves as the single source of truth, preserving the data's fidelity."

    CẤM: ép kiểu, đổi tên, lọc, JOIN, dedup — tất cả thuộc về silver.
    ĐƯỢC: thêm cột provenance/metadata (Microsoft cho phép, ví dụ _metadata.file_name).

    Nguồn: postgres.public.administrative_units (DuckDB postgres extension, ATTACH read_only)
*/

SELECT
    *,
    'postgres.public.administrative_units'  AS _source,
    CURRENT_TIMESTAMP         AS _ingested_at
FROM {{ source('pg_source', 'administrative_units') }}
