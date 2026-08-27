SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT ward_key) AS ward_count,
    COUNT(*) FILTER (WHERE grid_cell_id IS NULL) AS missing_grid_count
FROM {{ ref('bridge_hanoi_ward_forecast_grid') }}
HAVING
    COUNT(*) <> 126
    OR COUNT(DISTINCT ward_key) <> 126
    OR COUNT(*) FILTER (WHERE grid_cell_id IS NULL) <> 0
