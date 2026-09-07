/*
    MART — một dòng feature cho mỗi quan sát ngập đủ điều kiện huấn luyện.

    Grain: `observation_id`. Nhãn (`is_flooded`, `traffic_status`) đến từ
    `fct_flood_event_observation`; toàn bộ feature còn lại đến từ archive tại
    ĐÚNG thời điểm quan sát.

    ─── Quy tắc thời gian ───────────────────────────────────────────────────
    Mọi cửa sổ mưa KẾT THÚC tại giờ quan sát, không phải tổng cả ngày. Dòng ghi
    "lúc 07h00" phải ghép với mưa TRƯỚC 07h00. `rain_{N}h_mm` trong
    `fct_rain_archive_hourly` đã có sẵn ngữ nghĩa (t−N, t], nên chỉ cần khớp
    đúng `valid_time_utc = giờ chứa observed_at_utc` là xong — không được tự
    cộng lại, và không được lấy giá trị của giờ muộn hơn.

    ─── Vì sao chỉ lấy is_training_eligible ────────────────────────────────
    Quan sát hạng D hoặc chỉ biết tới NGÀY vẫn nằm trong bảng nhãn, nhưng ghép
    chúng với mưa theo giờ là gán sai thời điểm. Sai thời điểm ở đây tệ hơn
    thiếu dòng: nó dạy mô hình rằng ngập xảy ra ở mức mưa mà thực tế chưa xảy ra.

    ─── Feature CHƯA có, và không được giả ─────────────────────────────────
    Công thức tổn thương đầy đủ cần thêm tỷ lệ bề mặt không thấm, địa hình thấp
    tương đối và độ nhạy lưu vực thoát nước. Repo hiện KHÔNG có DEM, không có
    land cover, không có sơ đồ cống. `dim_grid.grid_elevation_m` là độ cao ô
    lưới ~9 km — nó không phải cao độ tương đối của một tuyến phố, và dùng nó
    thay `relative_elevation` sẽ là một cột trông đúng nhưng vô nghĩa. Ba biến
    đó vắng mặt ở đây một cách có chủ ý.
*/

{{ config(
    materialized = 'table',
    tags = ['fact', 'flood']
) }}

{% set windows = rain_windows() %}

WITH observation AS (
    SELECT *
    FROM {{ ref('fct_flood_event_observation') }}
    WHERE is_active = TRUE
      AND is_training_eligible = TRUE
),

located AS (
    /*
        Gắn ô lưới theo model đúng với thời kỳ của quan sát.

        Mốc 2017 không phải tuỳ chọn: ECMWF IFS KHÔNG có dữ liệu trước 2017 và
        ERA5 (0,25°) là chuỗi duy nhất phủ 2000–2016. Trộn hai model trong cùng
        một tập train là trộn hai phân phối mưa khác nhau — docs/05 §6.
    */
    SELECT
        o.*,
        DATE_TRUNC('hour', o.observed_at_utc) AS forcing_hour_utc,
        bwg.weather_model,
        bwg.grid_cell_id
    FROM observation o
    JOIN {{ ref('bridge_ward_grid') }} bwg
        ON bwg.ward_code = o.ward_code
       AND bwg.is_active = TRUE
       AND bwg.weather_model = CASE
               WHEN o.observed_at_utc >= CAST('2017-01-01 00:00:00+00' AS TIMESTAMPTZ)
                   THEN 'ecmwf_ifs'
               ELSE 'era5'
           END
),

forcing AS (
    -- Cửa sổ trượt đã tính sẵn ở fact archive, khớp đúng một giờ.
    SELECT
        l.observation_id,
        {%- for hours in windows %}
        f.rain_{{ hours }}h_mm,
        {%- endfor %}
        f.precipitation_mm AS rain_at_observation_mm,
        f.soil_moisture_0_to_7cm,
        f.soil_moisture_7_to_28cm,
        f.hanoi_rain_scenario_band,
        f.vn_rain_band_12h,
        f.vn_rain_band_24h
    FROM located l
    LEFT JOIN {{ ref('fct_rain_archive_hourly') }} f
        ON f.grid_cell_id = l.grid_cell_id
       AND f.valid_time_utc = l.forcing_hour_utc
),

antecedent AS (
    /*
        Hình dạng của trận mưa, không chỉ tổng lượng.

        Cùng 100 mm, dồn trong hai giờ hay rải đều một ngày cho kết quả thoát
        nước khác hẳn nhau; tổng tích luỹ một mình không phân biệt được.
    */
    SELECT
        l.observation_id,
        MAX(f.rain_1h_mm) FILTER (
            WHERE f.valid_time_utc > l.forcing_hour_utc - INTERVAL 6 HOUR
        ) AS max_rain_1h_previous_6h_mm,
        COUNT(*) FILTER (
            WHERE f.precipitation_mm > 0.1
              AND f.valid_time_utc > l.forcing_hour_utc - INTERVAL 24 HOUR
        ) AS wet_hours_previous_24h,
        -- NULL = suốt 72 giờ trước đó không có giờ nào đạt 10 mm.
        DATE_DIFF(
            'hour',
            MAX(f.valid_time_utc) FILTER (WHERE f.precipitation_mm >= 10),
            l.forcing_hour_utc
        ) AS hours_since_rain_10mm
    FROM located l
    JOIN {{ ref('fct_rain_archive_hourly') }} f
        ON f.grid_cell_id = l.grid_cell_id
       AND f.valid_time_utc > l.forcing_hour_utc - INTERVAL 72 HOUR
       AND f.valid_time_utc <= l.forcing_hour_utc
    -- `forcing_hour_utc` phụ thuộc hàm vào `observation_id`; group cả hai chỉ
    -- để mốc so sánh trong FILTER/DATE_DIFF hợp lệ, không đổi grain.
    GROUP BY l.observation_id, l.forcing_hour_utc
),

vulnerability AS (
    /*
        Tần suất ngập lịch sử của địa điểm — biến đại diện cho phần hạ tầng
        (điểm trũng, cống yếu, bê tông hoá) mà dự án không quan sát trực tiếp.

        LEAVE-ONE-EVENT-OUT, và đây là chỗ dễ rò rỉ nhãn nhất trong cả pipeline:
        nếu đếm cả trận đang xét, một địa điểm ngập trong trận đó sẽ tự nâng
        "tần suất lịch sử" của mình lên, và mô hình học được chính cái nhãn nó
        phải dự đoán. Backtest sẽ đẹp một cách vô nghĩa.

        Loại theo `event_id` chứ không chỉ theo mốc thời gian: các quan sát
        trong cùng một trận mưa có quan hệ với nhau, nên chỉ chặn "quan sát
        muộn hơn" là chưa đủ.
    */
    SELECT
        l.observation_id,
        COUNT(prior.observation_id) AS prior_observation_count,
        COUNT(prior.observation_id) FILTER (WHERE prior.is_flooded) AS prior_flood_count,
        CAST(COUNT(prior.observation_id) FILTER (WHERE prior.is_flooded) AS DOUBLE)
            / NULLIF(COUNT(prior.observation_id), 0) AS historical_flood_frequency,
        AVG(prior.depth_max_cm) FILTER (WHERE prior.is_flooded) AS historical_mean_depth_cm,
        AVG(CASE WHEN prior.traffic_status = 'impassable' THEN 1.0 ELSE 0.0 END)
            FILTER (WHERE prior.is_flooded) AS historical_impassable_rate
    FROM located l
    LEFT JOIN {{ ref('fct_flood_event_observation') }} prior
        ON prior.location_key = l.location_key
       AND prior.event_id <> l.event_id
       AND prior.observed_at_utc < l.observed_at_utc
       AND prior.is_active = TRUE
    GROUP BY l.observation_id
),

catalogue_by_ward AS (
    /*
        `dim_flood_point` có thể có nhiều điểm trong cùng một phường. Join trực
        tiếp dimension này vào grain observation sẽ nhân dòng (đã thấy hai
        điểm ở Thanh Xuân và hai điểm ở Yên Hoà). Aggregate về đúng một dòng
        mỗi ward trước khi nối để bảo toàn grain `observation_id`.

        `catalogue_rain_scenario` lấy kịch bản nhạy nhất trong phường; drainage
        basin chỉ công bố một tên khi mọi điểm cùng lưu vực, nếu không ghi rõ
        `multiple` thay vì chọn ngẫu nhiên một dòng.
    */
    SELECT
        ward_code,
        COUNT(*) AS catalogue_point_count,
        CASE
            WHEN COUNT(DISTINCT drainage_basin) = 1 THEN MIN(drainage_basin)
            ELSE 'multiple'
        END AS drainage_basin,
        CASE MIN(
            CASE rain_scenario
                WHEN 'scenario_50_70mm' THEN 1
                WHEN 'scenario_70_100mm' THEN 2
                WHEN 'scenario_over_100mm' THEN 3
                ELSE 4
            END
        )
            WHEN 1 THEN 'scenario_50_70mm'
            WHEN 2 THEN 'scenario_70_100mm'
            WHEN 3 THEN 'scenario_over_100mm'
        END AS catalogue_rain_scenario
    FROM {{ ref('dim_flood_point') }}
    WHERE is_active = TRUE AND status = 'active'
    GROUP BY ward_code
)

SELECT
    l.observation_id,
    l.event_id,
    l.location_key,
    l.location_name_raw,
    l.ward_code,
    l.weather_model,
    l.grid_cell_id,
    l.observed_at_utc,
    l.forcing_hour_utc,

    -- ── Nhãn ────────────────────────────────────────────────────────────
    l.is_flooded,
    l.traffic_status,
    l.depth_min_cm,
    l.depth_max_cm,
    l.source_grade,

    -- ── Feature mưa động ────────────────────────────────────────────────
    fo.rain_at_observation_mm,
    {%- for hours in windows %}
    fo.rain_{{ hours }}h_mm,
    {%- endfor %}
    a.max_rain_1h_previous_6h_mm,
    a.wet_hours_previous_24h,
    a.hours_since_rain_10mm,

    -- ── Độ ẩm đất ───────────────────────────────────────────────────────
    --
    -- Giá trị thô gần như vô dụng ở đây: trong trận 07/10/2025 nó nằm phẳng ở
    -- 0,430–0,439 và đã gần bão hoà TRƯỚC khi mưa bắt đầu. Chỉ vị trí trong
    -- phân phối lịch sử của chính ô đó, tháng đó, mới mang thông tin — cùng
    -- điểm Hoàn Kiếm, 0,414 thô tương ứng SMI 0,95.
    fo.soil_moisture_0_to_7cm,
    fo.soil_moisture_7_to_28cm,
    {{ normalize_p10_p90(
        'fo.soil_moisture_0_to_7cm',
        'clim.soil_moisture_p10',
        'clim.soil_moisture_p90'
    ) }} AS soil_moisture_index,

    -- ── Mưa đã chuẩn hoá theo lịch sử của chính ô lưới + tháng ──────────
    {%- for hours in windows %}
    {{ normalize_by_p99(
        'fo.rain_' ~ hours ~ 'h_mm',
        'clim.rain_' ~ hours ~ 'h_p99_mm'
    ) }} AS rain_{{ hours }}h_normalized,
    {%- endfor %}

    -- ── Dễ tổn thương (leave-one-event-out) ─────────────────────────────
    v.prior_observation_count,
    v.prior_flood_count,
    v.historical_flood_frequency,
    v.historical_mean_depth_cm,
    v.historical_impassable_rate,
    catalogue.drainage_basin,
    catalogue.catalogue_rain_scenario,
    COALESCE(catalogue.catalogue_point_count, 0) AS catalogue_point_count,
    -- Chỉ báo 0/1 chứ không phải BOOLEAN: cột này tồn tại để đi vào tổng có
    -- trọng số của V, và phải cùng kiểu với cột cùng tên ở fct_flood_risk_score.
    CAST(CASE WHEN catalogue.ward_code IS NOT NULL THEN 1.0 ELSE 0.0 END AS DOUBLE)
        AS is_known_flood_point,

    -- ── Dải kịch bản hiện hành, để so mô hình với ngưỡng đang dùng ──────
    fo.hanoi_rain_scenario_band,
    fo.vn_rain_band_12h,
    fo.vn_rain_band_24h,

    {{ processing_updated_at() }} AS _updated_at
FROM located l
LEFT JOIN forcing fo USING (observation_id)
LEFT JOIN antecedent a USING (observation_id)
LEFT JOIN vulnerability v USING (observation_id)
LEFT JOIN {{ ref('fct_rain_climatology') }} clim
    ON clim.grid_cell_id = l.grid_cell_id
   AND clim.calendar_month = MONTH(l.forcing_hour_utc)
{#
    Điểm danh mục QĐ 2280 ở CÙNG phường. Nguồn quan sát không ghi point_id, và
    ghép theo tên đoạn đường thì không đáng tin, nên chỉ dùng được ở mức phường.
#}
LEFT JOIN catalogue_by_ward catalogue
    ON catalogue.ward_code = l.ward_code
