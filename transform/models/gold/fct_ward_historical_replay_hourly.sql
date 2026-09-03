{{ config(materialized = 'table', tags = ['gold', 'kpi', 'rainfall', 'historical', 'replay', 'hanoi']) }}

SELECT
    MD5(CONCAT_WS(
        '|', windows.replay_window_id, CAST(mapping.ward_key AS VARCHAR),
        historical.historical_hourly_key
    )) AS ward_replay_hourly_key,
    windows.replay_window_id,
    windows.window_start_utc,
    windows.window_end_utc,
    windows.scenario_name,
    windows.scenario_context,
    windows.declaration_source,
    windows.replay_definition_version,
    'nq_1656_2025' AS as_of_boundary_version,
    mapping.ward_key,
    mapping.ward_code,
    mapping.ward_name,
    mapping.requested_latitude,
    mapping.requested_longitude,
    mapping.mapping_distance_km,
    historical.*,
    events.rainfall_event_id,
    events.event_start_utc,
    events.event_end_utc,
    events.event_definition_version
FROM {{ ref('dim_historical_replay_window') }} AS windows
INNER JOIN {{ ref('fct_rainfall_historical_hourly') }} AS historical
    ON historical.valid_time_utc >= windows.window_start_utc
   AND historical.valid_time_utc < windows.window_end_utc
INNER JOIN {{ ref('ward_grid_map') }} AS mapping
    ON historical.grid_cell_id = mapping.grid_cell_id
LEFT JOIN {{ ref('fct_rainfall_historical_event') }} AS events
    ON historical.grid_cell_id = events.grid_cell_id
   AND historical.valid_time_utc BETWEEN events.event_start_utc AND events.event_end_utc
