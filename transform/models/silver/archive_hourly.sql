{{ config(materialized = 'view') }}

/*
    SILVER — dedup và conform lịch sử ERA5 theo giờ.

    Backfill từng chạy lại nhiều lần cho cùng một tháng nên bronze có trùng lặp
    rất lớn (đo 2026-08-21: 3.062.736 dòng thô). Dedup theo (ô lưới, giờ).
*/

SELECT * EXCLUDE (rn)
FROM (
    SELECT
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY grid_latitude, grid_longitude, valid_time_utc
            ORDER BY _ingested_at DESC, _source_file DESC
        ) AS rn
    FROM {{ source('bronze_weather', 'open_meteo_archive') }}
)
WHERE rn = 1
