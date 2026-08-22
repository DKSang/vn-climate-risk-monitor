{{ config(materialized = 'view') }}

/*
    SILVER — dedup và conform dự báo theo giờ.

    Bronze giữ mọi retrieval vintage (mỗi giờ fetch lại 72h tới nên các slot
    chồng lấn nhau). Silver giữ bản MỚI NHẤT cho mỗi (ô lưới, giờ hợp lệ).
*/

SELECT * EXCLUDE (rn)
FROM (
    SELECT
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        showers_mm,
        precipitation_probability_pct,
        weather_code,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY grid_latitude, grid_longitude, valid_time_utc
            ORDER BY _ingested_at DESC, _source_file DESC
        ) AS rn
    FROM {{ source('bronze_weather', 'open_meteo_forecast') }}
)
WHERE rn = 1
