/* Ward ↔ weather grid mapping by model. */
{{ config(
    materialized = 'table',
    tags = ['bridge', 'forecast', 'archive']
) }}

WITH archive_map AS (
    SELECT
        ward_code,
        weather_model,
        grid_cell_id,
        grid_elevation_m
    FROM {{ ref('stg_seed__ward_grid') }}
),

-- Forecast mesh lấy từ run mới nhất; ward map vào grid gần nhất.
latest_forecast_run AS (
    SELECT REGEXP_EXTRACT(
        _source_file,
        '/incremental/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/(run_[0-9]{8}T[0-9]{6})/',
        1
    ) AS forecast_run_id
    FROM {{ source('silver_staging', 'stg_weather_forecast') }}
    GROUP BY forecast_run_id
    ORDER BY MAX(_ingested_at) DESC, forecast_run_id DESC
    LIMIT 1
),

current_forecast_grids AS (
    SELECT DISTINCT
        {{ grid_cell_id("'ecmwf_ifs_fc'", 'ROUND(grid_latitude, 6)', 'ROUND(grid_longitude, 6)') }} AS grid_cell_id,
        grid_latitude,
        grid_longitude
    FROM {{ source('silver_staging', 'stg_weather_forecast') }}
    WHERE REGEXP_EXTRACT(
        _source_file,
        '/incremental/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/(run_[0-9]{8}T[0-9]{6})/',
        1
    ) = (SELECT forecast_run_id FROM latest_forecast_run)
),

forecast_ranked AS (
    SELECT
        ward.ward_code,
        'ecmwf_ifs_fc' AS weather_model,
        grid.grid_cell_id,
        CAST(NULL AS DOUBLE) AS grid_elevation_m,
        ROW_NUMBER() OVER (
            PARTITION BY ward.ward_code
            ORDER BY
                POWER(ward.ward_latitude - grid.grid_latitude, 2)
                + POWER(
                    (ward.ward_longitude - grid.grid_longitude)
                    * COS(RADIANS(ward.ward_latitude)),
                    2
                ),
                grid.grid_cell_id
        ) AS proximity_rank
    FROM {{ ref('stg_seed__ward') }} AS ward
    CROSS JOIN current_forecast_grids AS grid
),

forecast_map AS (
    SELECT
        ward_code,
        weather_model,
        grid_cell_id,
        grid_elevation_m
    FROM forecast_ranked
    WHERE proximity_rank = 1
),

all_maps AS (
    SELECT * FROM archive_map
    UNION ALL
    SELECT * FROM forecast_map
)

SELECT
    MD5(CONCAT_WS('|', map.ward_code, map.weather_model)) AS ward_grid_key,
    map.ward_code,
    map.weather_model,
    map.grid_cell_id,
    map.grid_elevation_m,
    COUNT(*) OVER (PARTITION BY map.weather_model, map.grid_cell_id)
        AS ward_count_on_grid,
    TRUE AS is_active,
    CAST(NULL AS TIMESTAMPTZ) AS _deactivated_at,
    CURRENT_TIMESTAMP AS _updated_at
FROM all_maps AS map
INNER JOIN {{ ref('dim_grid') }} AS grid
    ON grid.grid_cell_id = map.grid_cell_id
