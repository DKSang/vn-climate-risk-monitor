{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'hanoi']) }}

/* GOLD serving projection — không aggregate lại các phường dùng chung grid. */

SELECT
    bridge.ward_key,
    bridge.ward_code,
    bridge.ward_name,
    bridge.requested_latitude,
    bridge.requested_longitude,
    bridge.mapping_distance_km,
    rainfall.*
FROM {{ ref('bridge_hanoi_ward_forecast_grid') }} AS bridge
INNER JOIN {{ ref('fct_rainfall_forecast_hourly') }} AS rainfall
    ON bridge.forecast_snapshot_id = rainfall.forecast_snapshot_id
   AND bridge.grid_cell_id = rainfall.grid_cell_id
