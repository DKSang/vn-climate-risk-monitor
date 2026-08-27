-- Transform cho nguồn open_meteo_archive.
-- Chạy bởi autoloader engine; {{ files }} được thay bằng danh sách file đã claim.
--
-- Thay thế archive_parser.py (401 dòng Python + pyarrow). Toàn bộ explode và ép kiểu
-- do DuckDB làm.
--
-- Rescue (tinh thần _rescued_data của Auto Loader): dùng TRY_CAST thay CAST, giá
-- trị hỏng thành NULL và được ghi lại trong _rescued_data thay vì làm gãy cả lô.

WITH raw AS (
    SELECT *
    FROM read_json_auto(
        {{ files }},
        filename = true,
        union_by_name = true,
        maximum_object_size = 209715200
    )
),

exploded AS (
    SELECT
        filename,
        latitude,
        longitude,
        elevation,
        timezone,
        utc_offset_seconds,
        hourly_units,
        -- payload lỗi của Open-Meteo (rate limit...) không có `hourly`
        UNNEST(hourly.time)                      AS t_raw,
        UNNEST(hourly.precipitation)             AS precipitation_raw,
        UNNEST(hourly.rain)                      AS rain_raw,
        UNNEST(hourly.weather_code)              AS weather_code_raw,
        UNNEST(hourly.soil_moisture_0_to_7cm)    AS sm_0_7_raw,
        UNNEST(hourly.soil_moisture_7_to_28cm)   AS sm_7_28_raw
    FROM raw
    WHERE hourly IS NOT NULL
)

SELECT
    TRY_CAST(latitude  AS DOUBLE)                     AS grid_latitude,
    TRY_CAST(longitude AS DOUBLE)                     AS grid_longitude,
    TRY_CAST(elevation AS DOUBLE)                     AS elevation_m,
    TO_TIMESTAMP(TRY_CAST(CAST(t_raw AS VARCHAR) AS BIGINT)) AS valid_time_utc,
    TRY_CAST(precipitation_raw AS DOUBLE)             AS precipitation_mm,
    TRY_CAST(rain_raw          AS DOUBLE)             AS rain_mm,
    TRY_CAST(weather_code_raw  AS INTEGER)            AS weather_code,
    TRY_CAST(sm_0_7_raw        AS DOUBLE)             AS soil_moisture_0_to_7cm,
    TRY_CAST(sm_7_28_raw       AS DOUBLE)             AS soil_moisture_7_to_28cm,
    CAST(timezone AS VARCHAR)                         AS timezone,
    TRY_CAST(utc_offset_seconds AS INTEGER)           AS utc_offset_seconds,
    CAST(hourly_units AS VARCHAR)                     AS hourly_units_json,
    filename                                          AS _source_file,
    CURRENT_TIMESTAMP                                 AS _ingested_at,
    -- _rescued_data: ghi lại giá trị KHÔNG ép kiểu được, thay vì fail cả lô
    NULLIF(
        TRIM(
            CASE WHEN TRY_CAST(CAST(t_raw AS VARCHAR) AS BIGINT) IS NULL
                 THEN 'time=' || CAST(t_raw AS VARCHAR) || ' ' ELSE '' END ||
            CASE WHEN precipitation_raw IS NOT NULL
                  AND TRY_CAST(precipitation_raw AS DOUBLE) IS NULL
                 THEN 'precipitation=' || CAST(precipitation_raw AS VARCHAR) || ' ' ELSE '' END ||
            CASE WHEN weather_code_raw IS NOT NULL
                  AND TRY_CAST(weather_code_raw AS INTEGER) IS NULL
                 THEN 'weather_code=' || CAST(weather_code_raw AS VARCHAR) ELSE '' END
        ), ''
    )                                                 AS _rescued_data
FROM exploded
