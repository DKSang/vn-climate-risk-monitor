{{ config(materialized = 'view') }}

/*
    Projection nghiệp vụ từ centroid phường sang returned grid gần nhất trong
    snapshot forecast hiện hành. Scope theo snapshot để không trộn grid mesh của
    các model/vintage Best Match khác nhau.
*/

WITH grids AS (
    SELECT DISTINCT
        forecast_snapshot_id,
        grid_cell_id,
        grid_latitude,
        grid_longitude
    FROM {{ ref('forecast_hourly') }}
),

candidates AS (
    SELECT
        ward.ward_key,
        ward.ward_code,
        ward.ward_name,
        ward.latitude AS requested_latitude,
        ward.longitude AS requested_longitude,
        grid.forecast_snapshot_id,
        grid.grid_cell_id,
        grid.grid_latitude,
        grid.grid_longitude,
        111.32 * SQRT(
            POWER(ward.latitude - grid.grid_latitude, 2)
            + POWER(
                COS(RADIANS(ward.latitude))
                * (ward.longitude - grid.grid_longitude),
                2
            )
        ) AS mapping_distance_km
    FROM {{ ref('dim_hanoi_ward') }} AS ward
    CROSS JOIN grids AS grid
)

SELECT
    ward_key,
    ward_code,
    ward_name,
    requested_latitude,
    requested_longitude,
    forecast_snapshot_id,
    grid_cell_id,
    grid_latitude,
    grid_longitude,
    mapping_distance_km
FROM candidates
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY forecast_snapshot_id, ward_key
    ORDER BY mapping_distance_km, grid_cell_id
) = 1
