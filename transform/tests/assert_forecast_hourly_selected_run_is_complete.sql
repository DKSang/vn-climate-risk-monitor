-- Kiểm tra completeness ở grain Bronze trước khi các ward trùng returned grid
-- bị collapse trong forecast_hourly. Không giả định batch size hoặc số file.
WITH selected_source_files AS (
    SELECT UNNEST(STRING_SPLIT(source_object_keys, '|')) AS source_file
    FROM {{ ref('forecast_hourly') }}
),

selected_runs AS (
    SELECT DISTINCT
        REGEXP_EXTRACT(
            source_file,
            '/(run_[0-9]{8}T[0-9]{6})/',
            1
        ) AS run_name
    FROM selected_source_files
),

selected_snapshots AS (
    SELECT DISTINCT forecast_snapshot_id
    FROM {{ ref('forecast_hourly') }}
),

source_rows AS (
    SELECT
        *,
        REGEXP_EXTRACT(
            _source_file,
            '/(run_[0-9]{8}T[0-9]{6})/',
            1
        ) AS run_name
    FROM {{ source('bronze_weather', 'open_meteo_forecast') }}
),

latest_file_ingests AS (
    SELECT *
    FROM source_rows
    WHERE run_name <> ''
    QUALIFY _ingested_at = MAX(_ingested_at) OVER (PARTITION BY _source_file)
),

selected_rows AS (
    SELECT source.*
    FROM latest_file_ingests AS source
    INNER JOIN selected_runs AS selected
        ON source.run_name = selected.run_name
),

run_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT valid_time_utc) AS horizon_hour_count
    FROM selected_rows
),

hour_counts AS (
    SELECT valid_time_utc, COUNT(*) AS location_count
    FROM selected_rows
    GROUP BY valid_time_utc
),

file_hour_counts AS (
    SELECT
        _source_file,
        valid_time_utc,
        COUNT(*) AS location_count
    FROM selected_rows
    GROUP BY _source_file, valid_time_utc
),

file_metrics AS (
    SELECT
        _source_file,
        COUNT(*) AS observed_hour_count,
        MIN(location_count) AS min_location_count,
        MAX(location_count) AS max_location_count
    FROM file_hour_counts
    GROUP BY _source_file
),

ward_count AS (
    SELECT COUNT(*) AS expected_ward_count
    FROM {{ ref('dim_hanoi_ward') }}
)

SELECT
    metrics.row_count,
    metrics.horizon_hour_count,
    wards.expected_ward_count
FROM run_metrics AS metrics
CROSS JOIN ward_count AS wards
WHERE (SELECT COUNT(*) FROM selected_runs WHERE run_name <> '') <> 1
   OR (SELECT COUNT(*) FROM selected_snapshots) <> 1
   OR EXISTS (
       SELECT 1
       FROM selected_runs AS run
       CROSS JOIN selected_snapshots AS snapshot
       WHERE snapshot.forecast_snapshot_id
           <> 'forecast_' || REPLACE(run.run_name, 'run_', '')
   )
   OR metrics.horizon_hour_count = 0
   OR metrics.row_count <> wards.expected_ward_count * metrics.horizon_hour_count
   OR EXISTS (
       SELECT 1
       FROM hour_counts AS hourly
       WHERE hourly.location_count <> wards.expected_ward_count
   )
   OR EXISTS (
       SELECT 1
       FROM file_metrics AS file
       WHERE file.observed_hour_count <> metrics.horizon_hour_count
          OR file.min_location_count <> file.max_location_count
   )
