{{ config(materialized = 'table') }}

/*
    BRONZE — toạ độ centroid từ artifact tĩnh được version-control bằng dbt seed.
    Giữ nguyên mã zero-pad và chỉ bổ sung provenance.
*/

SELECT
    *,
    'dbt seed: ward_coordinates_seed.csv' AS _source,
    CURRENT_TIMESTAMP AS _ingested_at
FROM {{ ref('ward_coordinates_seed') }}
