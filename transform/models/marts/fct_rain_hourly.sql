/*
    MART — mưa theo ô lưới × giờ, kèm cửa sổ trượt và dải kịch bản.

    Bảng lớn nhất (~20M dòng) và là bảng DUY NHẤT incremental: hai fact còn lại
    dựng từ đây, nhỏ, nên full refresh rẻ hơn là nuôi thêm hai checkpoint.

    `rain_{N}h_mm` NULL nghĩa là cửa sổ THIẾU GIỜ. Không có cột `_is_complete`
    song song — NULL đã mang đúng nghĩa đó, và 14 cột phụ của bản cũ chỉ là
    cùng một thông tin viết lại ba lần.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'rain_hourly_key',
    tags = ['fact', 'rain']
) }}

{#
    Lookback SUY RA từ danh sách cửa sổ, không gõ tay: cửa sổ rộng nhất N giờ
    thì một giờ mới ở T làm sai các dòng đầu ra trong [T, T+(N−1)h], và để
    tính chúng phải đọc từ T−(N−1)h. Thêm cửa sổ 72h sau này thì lookback tự
    đúng theo — không có chỗ nào để quên cập nhật.
#}
{% set windows = rain_windows() %}
{% set lookback = (windows | max - 1) ~ ' hours' %}

WITH source AS (
    SELECT *
    FROM {{ ref('int_weather_hourly') }}
    {{ incremental_input_scope(
        relation = ref('int_weather_hourly'),
        source_ref = 'int_weather_hourly',
        dimension = 'valid_time_utc',
        change_column = '_updated_at',
        expand_backward = lookback,
        expand_forward = lookback,
        keys = ['grid_cell_id']
    ) }}
),

windowed AS (
    SELECT
        grid_cell_id,
        valid_time_utc,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        _source_file,
        _ingested_at,
        _updated_at,
        {{ rolling_rain_sums(windows, partition_by='grid_cell_id') }}
    FROM source
),

published AS (
    SELECT
        MD5(CONCAT_WS('|', grid_cell_id, CAST(valid_time_utc AS VARCHAR)))
            AS rain_hourly_key,
        grid_cell_id,
        valid_time_utc,
        CAST(valid_time_utc AS DATE) AS rain_date,
        precipitation_mm,
        rain_mm,
        weather_code,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm,
        {{ rolling_rain_columns(windows) }},
        _source_file,
        _ingested_at,
        _updated_at
    FROM windowed
)

SELECT
    *,
    {{ hanoi_rain_scenario_band('rain_1h_mm') }} AS hanoi_rain_scenario_band,
    {{ vn_rain_band_12h('rain_12h_mm') }} AS vn_rain_band_12h,
    {{ vn_rain_band_24h('rain_24h_mm') }} AS vn_rain_band_24h
FROM published
{{ incremental_output_scope(
    relation = ref('int_weather_hourly'),
    source_ref = 'int_weather_hourly',
    dimension = 'valid_time_utc',
    change_column = '_updated_at',
    expand_forward = lookback
) }}
