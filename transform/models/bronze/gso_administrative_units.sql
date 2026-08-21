{{ config(materialized = 'table') }}

/*
    BRONZE — bản sao append/replayable của loại đơn vị hành chính từ Postgres.
    Chỉ bổ sung provenance; không ép kiểu, lọc, join hoặc deduplicate.
*/

SELECT
    *,
    'postgres.public.administrative_units' AS _source,
    CURRENT_TIMESTAMP AS _ingested_at
FROM {{ source('pg_source', 'administrative_units') }}
