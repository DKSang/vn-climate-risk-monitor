{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'hanoi']) }}

SELECT
    bridge.ward_key,
    bridge.ward_code,
    bridge.ward_name,
    bridge.requested_latitude,
    bridge.requested_longitude,
    bridge.mapping_distance_km,
    summary.*
FROM {{ ref('bridge_hanoi_ward_forecast_grid') }} AS bridge
INNER JOIN {{ ref('fct_rainfall_forecast_summary') }} AS summary
    ON bridge.forecast_snapshot_id = summary.forecast_snapshot_id
   AND bridge.grid_cell_id = summary.grid_cell_id
