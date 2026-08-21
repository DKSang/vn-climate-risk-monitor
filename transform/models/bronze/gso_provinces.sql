{{ config(materialized = 'table') }}

/*
    BRONZE — bản sao append/replayable của danh mục tỉnh/thành từ Postgres.
    Chỉ bổ sung provenance; không ép kiểu, lọc, join hoặc deduplicate.
*/

SELECT
    *,
    'postgres.public.provinces' AS _source,
    CURRENT_TIMESTAMP AS _ingested_at
FROM {{ source('pg_source', 'provinces') }}
