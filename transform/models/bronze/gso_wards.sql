{{ config(materialized = 'table') }}

/*
    BRONZE — bản sao append/replayable của danh mục phường/xã từ Postgres.
    Tên model có source qualifier để không xung đột với entity chuẩn hóa ở Silver.
*/

SELECT
    *,
    'postgres.public.wards' AS _source,
    CURRENT_TIMESTAMP AS _ingested_at
FROM {{ source('pg_source', 'wards') }}
