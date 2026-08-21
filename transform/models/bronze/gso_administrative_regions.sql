{{ config(materialized = 'table') }}

/*
    BRONZE — bản sao append/replayable của vùng hành chính từ Postgres nguồn.
    Chỉ bổ sung provenance; không ép kiểu, lọc, join hoặc deduplicate.
*/

SELECT
    *,
    'postgres.public.administrative_regions' AS _source,
    CURRENT_TIMESTAMP AS _ingested_at
FROM {{ source('pg_source', 'administrative_regions') }}
