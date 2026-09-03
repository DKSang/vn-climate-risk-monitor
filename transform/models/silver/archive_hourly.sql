{{ config(materialized = 'view') }}

/*
    SILVER — dedup và conform lịch sử theo giờ, GỘP HAI MODEL.

    era5      0,25° (12 ô Hà Nội), trước 2017. Chuỗi lịch sử sâu duy nhất —
              IFS không có dữ liệu trước 2017 (probe: 2016 mọi quý NULL).
    ecmwf_ifs ~9km  (48 ô),        2017→nay. Chi tiết theo phường thật sự:
              cùng ngày mưa, ba phường mà ERA5 gộp thành một chuỗi 9,3mm thì IFS
              trả 105,0 / 137,9 / 116,2 mm.

    Hai model KHÔNG được trộn vào cùng một chuỗi — `weather_model` nằm trong khoá
    dedup và phải nằm trong mọi phép join phía sau. ecmwf_ifs là chuỗi phân tích
    nghiệp vụ chứ không phải reanalysis, nên đồng nhất theo thời gian kém hơn
    era5; so trực tiếp trung bình trước/sau mốc 2017 là sai.

    Tọa độ ROUND 6 chữ số trước khóa và mapping. weather_product = archive.
*/

{%- set ifs_relation = adapter.get_relation(
        database='bronze_store', schema='tables', identifier='open_meteo_ifs') -%}

WITH unioned AS (
    SELECT
        'era5' AS weather_model,
        'archive' AS weather_product,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at
    FROM {{ source('bronze_weather', 'open_meteo_archive') }}

    {%- if ifs_relation %}
    UNION ALL

    SELECT
        weather_model,
        'archive' AS weather_product,
        ROUND(grid_latitude, 6) AS grid_latitude,
        ROUND(grid_longitude, 6) AS grid_longitude,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at
    FROM {{ ifs_relation }}
    {%- endif %}
),

deduped AS (
    SELECT *
    FROM unioned
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY weather_model, grid_latitude, grid_longitude, valid_time_utc
        ORDER BY _ingested_at DESC, _source_file DESC
    ) = 1
)

SELECT
    {{ grid_cell_id('weather_model', "'archive'", 'grid_latitude', 'grid_longitude') }}
        AS grid_cell_id,
    *
FROM deduped
