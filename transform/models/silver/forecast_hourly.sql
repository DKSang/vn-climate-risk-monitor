{{ config(materialized = 'view') }}

/*
    SILVER — run forecast production mới nhất có đủ ward và đủ horizon.

    Object path mang run identity (`run_YYYYMMDDTHHMMSS`). Chọn toàn bộ một run
    thay vì chọn file theo hourly slot để không ghép response từ nhiều lần chạy.
    Bronze không giữ requested ward identity; do đó completeness được kiểm tra
    bằng tổng row, số row ở từng valid hour và horizon đồng nhất ở từng file.
*/

WITH source_rows AS (
    SELECT
        *,
        REGEXP_EXTRACT(
            _source_file,
            '/incremental/([0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2})/(run_[0-9]{8}T[0-9]{6})/',
            1
        ) AS slot_key,
        REGEXP_EXTRACT(
            _source_file,
            '/incremental/([0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2})/(run_[0-9]{8}T[0-9]{6})/',
            2
        ) AS run_name
    FROM {{ source('bronze_weather', 'open_meteo_forecast') }}
),

-- Autoloader có thể reinsert cùng object. Giữ lần ingest mới nhất của object,
-- nhưng không DISTINCT row vì nhiều ward có thể được trả về cùng một grid.
latest_file_ingests AS (
    SELECT *
    FROM source_rows
    WHERE slot_key <> '' AND run_name <> ''
    QUALIFY _ingested_at = MAX(_ingested_at) OVER (PARTITION BY _source_file)
),

ward_count AS (
    SELECT COUNT(*) AS expected_ward_count
    FROM {{ ref('dim_hanoi_ward') }}
),

run_stats AS (
    SELECT
        slot_key,
        run_name,
        COUNT(*) AS row_count,
        COUNT(DISTINCT valid_time_utc) AS horizon_hour_count
    FROM latest_file_ingests
    GROUP BY slot_key, run_name
),

run_hour_counts AS (
    SELECT
        slot_key,
        run_name,
        valid_time_utc,
        COUNT(*) AS location_count
    FROM latest_file_ingests
    GROUP BY slot_key, run_name, valid_time_utc
),

run_hour_consistency AS (
    SELECT
        slot_key,
        run_name,
        MIN(location_count) AS min_location_count,
        MAX(location_count) AS max_location_count
    FROM run_hour_counts
    GROUP BY slot_key, run_name
),

file_hour_counts AS (
    SELECT
        slot_key,
        run_name,
        _source_file,
        valid_time_utc,
        COUNT(*) AS location_count
    FROM latest_file_ingests
    GROUP BY slot_key, run_name, _source_file, valid_time_utc
),

file_consistency AS (
    SELECT
        slot_key,
        run_name,
        _source_file,
        COUNT(*) AS observed_hour_count,
        MIN(location_count) AS min_location_count,
        MAX(location_count) AS max_location_count
    FROM file_hour_counts
    GROUP BY slot_key, run_name, _source_file
),

complete_runs AS (
    SELECT stats.slot_key, stats.run_name
    FROM run_stats AS stats
    INNER JOIN run_hour_consistency AS hourly
        ON stats.slot_key = hourly.slot_key
       AND stats.run_name = hourly.run_name
    CROSS JOIN ward_count AS wards
    WHERE stats.horizon_hour_count > 0
      AND stats.row_count = wards.expected_ward_count * stats.horizon_hour_count
      AND hourly.min_location_count = wards.expected_ward_count
      AND hourly.max_location_count = wards.expected_ward_count
      AND NOT EXISTS (
          SELECT 1
          FROM file_consistency AS file
          WHERE file.slot_key = stats.slot_key
            AND file.run_name = stats.run_name
            AND (
                file.observed_hour_count <> stats.horizon_hour_count
                OR file.min_location_count <> file.max_location_count
            )
      )
),

selected_run AS (
    SELECT slot_key, run_name
    FROM complete_runs
    ORDER BY run_name DESC, slot_key DESC
    LIMIT 1
),

selected_rows AS (
    SELECT source.*
    FROM latest_file_ingests AS source
    INNER JOIN selected_run AS selected
        ON source.slot_key = selected.slot_key
       AND source.run_name = selected.run_name
)

SELECT
    'forecast_' || REPLACE(run_name, 'run_', '') AS forecast_snapshot_id,
    STRPTIME(slot_key, '%Y/%m/%d/%H') AT TIME ZONE 'UTC' AS as_of_utc,
    'ecmwf_ifs' AS weather_model,
    'forecast' AS weather_product,
    {{ grid_cell_id("'ecmwf_ifs'", "'forecast'", 'ROUND(grid_latitude, 6)', 'ROUND(grid_longitude, 6)') }}
        AS grid_cell_id,
    ROUND(grid_latitude, 6) AS grid_latitude,
    ROUND(grid_longitude, 6) AS grid_longitude,
    valid_time_utc,
    MAX(precipitation_mm) AS precipitation_mm,
    MAX(rain_mm) AS rain_mm,
    MAX(showers_mm) AS showers_mm,
    -- Probability có thể khác nhẹ giữa các requested point cùng returned grid;
    -- giữ giá trị bảo thủ nhất. KPI mưa MVP không dùng cột này để nhân lượng mưa.
    MAX(precipitation_probability_pct) AS precipitation_probability_pct,
    MAX(weather_code) AS weather_code,
    STRING_AGG(DISTINCT _source_file, '|' ORDER BY _source_file) AS source_object_keys,
    MAX(_ingested_at) AS _ingested_at
FROM selected_rows
GROUP BY
    slot_key,
    run_name,
    ROUND(grid_latitude, 6),
    ROUND(grid_longitude, 6),
    valid_time_utc
