/*
    INTERMEDIATE — một dòng hiện hành cho mỗi (ô lưới, giờ).
    (Lớp SILVER_CLEAN trong Silver Layer Flow: dedup + upsert change-aware.)

    Ba việc, đúng ba việc:

    1. DEDUP. Staging là append-only và giữ MỌI phiên bản đã fetch: cùng một
       (ô, giờ) có thể đến từ 5 file khác nhau vì cửa sổ backfill chồng nhau.
       Giữ lần ingest MỚI NHẤT — nó phản ánh lần fetch gần nhất.

    2. CANONICAL toạ độ. Round 6 số ĐÚNG MỘT LẦN rồi sinh `grid_cell_id`. Mọi
       lớp sau chỉ dùng id. Round hai lần ở hai chỗ là cách chắc chắn nhất để
       hai bảng không join được với nhau.

    3. MERGE CHANGE-AWARE. Chỉ ghi dòng có GIÁ TRỊ thật sự đổi.

    ── VÌ SAO (3) LÀ SỐNG CÒN ──────────────────────────────────────────────────
    Đo 2026-09-03: staging có 19.895.304 dòng trên 5.818.584 grain (70,75% là
    phiên bản lặp) nhưng **0 grain có giá trị mâu thuẫn**. Nếu MERGE chỉ so KEY
    thì mọi lần chạy sẽ ghi lại cả 5,8M dòng và bump `_updated_at` của tất cả →
    Gold thấy 5,8M dòng "vừa đổi" → reprocess toàn bộ → chuỗi incremental sụp
    ngay ở lần chạy thứ hai.

    So `_row_hash` khiến lần chạy không có dữ liệu mới ghi ĐÚNG 0 dòng.

    `_row_hash` CỐ Ý không gồm `_source_file`/`_ingested_at`: cùng một giá trị
    đến từ file khác không phải là dữ liệu đổi. Nhờ vậy `_source_file` giữ
    provenance của phiên bản đang nắm, không nhảy loạn theo mỗi lần re-fetch.

    ── VÌ SAO LÀ TABLE, KHÔNG PHẢI VIEW ────────────────────────────────────────
    Bản view có `QUALIFY` khiến DuckDB không đẩy được filter xuống dưới window
    function. Đo: lọc `_ingested_at > x` mất 9,7s qua view và 0,0s qua table.
    Macro incremental gọi subquery đó 5 lần mỗi lần chạy → ~49 giây chỉ để TÌM
    cửa sổ cần tính, trước khi tính một dòng nào.
*/

{{ config(
    materialized = 'incremental',
    unique_key = 'weather_hourly_key',
    tags = ['intermediate']
) }}

{#
    Cột GIÁ TRỊ dùng cho `_row_hash`. CỐ Ý KHÔNG có `elevation_m`.

    Đo 2026-09-03: cùng một ô lưới era5 ở cùng một giờ có elevation 9–41 m tuỳ
    dòng. Lý do: đợt fetch theo PHƯỜNG (2000–2013) hỏi toạ độ phường, Open-Meteo
    snap về cùng ô nhưng trả độ cao của ĐIỂM ĐƯỢC HỎI. Elevation vì vậy không
    phải thuộc tính của ô, và đưa nó vào hash làm hash lật theo dòng nào thắng
    dedup — 437.755 dòng bị bump giả ở lần chạy thứ hai.

    Độ cao cấp ô lấy ở `dim_grid` từ seed ánh xạ, nơi nó xác định được.
#}
{% set value_columns = [
    'precipitation_mm',
    'rain_mm',
    'weather_code',
    'soil_moisture_0_to_7cm',
    'soil_moisture_7_to_28cm',
] %}

{#
    `valid_time_utc IS NOT NULL` được bảo đảm ở staging view
    (`stg_open_meteo__weather_hourly`); không lặp lại ở đây.
#}
WITH staged AS (
    SELECT
        weather_model,
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
    FROM {{ ref('stg_open_meteo__weather_hourly') }}
    {{ incremental_changed_filter(
        source_ref = 'stg_weather_hourly',
        change_column = '_ingested_at',
        prefix = 'WHERE'
    ) }}
),

-- Grain là (model, ô, giờ). Cùng grain đến từ nhiều file thì bản nạp SAU thắng.
deduplicated AS (
    SELECT *
    FROM staged
    -- Thứ tự phải TOÀN PHẦN. Một file response của đợt fetch theo phường chứa
    -- nhiều dòng cho cùng (ô, giờ) — 1.852.200 tổ hợp hoà ở `_source_file`.
    -- Hoà mà không có tie-break thì ROW_NUMBER chọn tuỳ ý, và `_row_hash` lật
    -- giữa hai lần chạy dù dữ liệu không đổi.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY weather_model, grid_latitude, grid_longitude, valid_time_utc
        ORDER BY
            _ingested_at DESC,
            _source_file DESC
            {%- for column in value_columns %},
            {{ column }}
            {%- endfor %}
    ) = 1
),

incoming AS (
    SELECT
        MD5(CONCAT_WS(
            '|',
            {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }},
            CAST(valid_time_utc AS VARCHAR)
        )) AS weather_hourly_key,
        {{ grid_cell_id('weather_model', 'grid_latitude', 'grid_longitude') }}
            AS grid_cell_id,
        weather_model,
        grid_latitude,
        grid_longitude,
        valid_time_utc,
        {% for column in value_columns %}{{ column }},
        {% endfor %}
        MD5(CONCAT_WS('|',
            {%- for column in value_columns %}
            COALESCE(CAST({{ column }} AS VARCHAR), ''){{ "," if not loop.last }}
            {%- endfor %}
        )) AS _row_hash,
        _source_file,
        _ingested_at
    FROM deduplicated
)

SELECT
    incoming.*,
    -- Weather không bao giờ bị xoá ở nguồn (Open-Meteo là REST, không liệt kê
    -- được key để anti-join). Cột có mặt để mọi bảng clean cùng một hình dạng,
    -- consumer viết `WHERE is_active` mà không phải nhớ bảng nào hỗ trợ.
    TRUE AS is_active,
    {{ processing_updated_at() }} AS _updated_at
FROM incoming

{% if is_incremental() %}
-- Chỉ giữ dòng MỚI hoặc ĐỔI THẬT. Dòng trùng y hệt bị loại ở đây, nên chúng
-- không vào slice và `_updated_at` cũ của chúng được giữ nguyên.
LEFT JOIN {{ this }} AS existing
    ON existing.weather_hourly_key = incoming.weather_hourly_key
WHERE existing.weather_hourly_key IS NULL
   OR existing._row_hash <> incoming._row_hash
{% endif %}
