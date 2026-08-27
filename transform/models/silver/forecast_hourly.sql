{{ config(materialized = 'view') }}

/*
    SILVER — snapshot forecast production mới nhất đã land đủ 6 location batch.

    Không chọn "mới nhất theo từng grid × valid_time": cách đó ghép nhiều lần
    retrieval và có thể lấy một slot canary/partial. Lịch sử vintage vẫn còn ở
    Bronze qua _source_file; MVP chỉ cần một snapshot nhất quán để tính KPI.
*/

WITH source_rows AS (
    SELECT
        *,
        REGEXP_EXTRACT(
            _source_file,
            '/incremental/([0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2})/',
            1
        ) AS slot_key,
        REGEXP_EXTRACT(
            _source_file,
            '(response_[0-9]{3}[.]json)$',
            1
        ) AS batch_name
    FROM {{ source('bronze_weather', 'open_meteo_forecast') }}
),

file_inventory AS (
    SELECT DISTINCT slot_key, batch_name, _source_file, _ingested_at
    FROM source_rows
    WHERE slot_key <> '' AND batch_name <> ''
),

-- Slot mới nhất có đủ đúng 6 location batch; nếu một batch được retry trong
-- cùng logical slot, chỉ dùng object mới nhất.
selected_files AS (
    SELECT slot_key, batch_name, _source_file
    FROM file_inventory
    WHERE slot_key = (
        SELECT MAX(slot_key)
        FROM file_inventory
        GROUP BY slot_key
        HAVING COUNT(DISTINCT batch_name) = 6
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY slot_key, batch_name
        ORDER BY _ingested_at DESC, _source_file DESC
    ) = 1
),

selected_rows AS (
    SELECT source.*
    FROM source_rows AS source
    INNER JOIN selected_files AS file
        ON source.slot_key = file.slot_key
       AND source.batch_name = file.batch_name
       AND source._source_file = file._source_file
)

SELECT
    'forecast_' || REPLACE(slot_key, '/', '') AS forecast_snapshot_id,
    STRPTIME(slot_key, '%Y/%m/%d/%H') AT TIME ZONE 'UTC' AS as_of_utc,
    PRINTF('forecast_%.6f_%.6f', grid_latitude, grid_longitude) AS grid_cell_id,
    grid_latitude,
    grid_longitude,
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
GROUP BY slot_key, grid_latitude, grid_longitude, valid_time_utc
