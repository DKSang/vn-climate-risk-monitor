-- Ba bất biến của bảng feature. Cả ba đều thuộc loại lỗi cho ra số hợp lý mà
-- không test nào khác bắt được.
--
-- 1. Cửa sổ mưa phải kết thúc ĐÚNG tại giờ quan sát. Lệch một giờ là ghép nhãn
--    với lượng mưa chưa rơi — mô hình sẽ trông rất giỏi và hoàn toàn vô dụng.
-- 2. `historical_flood_frequency` phải leave-one-event-out. Nếu số quan sát
--    "trước đó" lại đếm cả trận đang xét thì đó là rò rỉ nhãn.
-- 3. Tỷ lệ và chỉ số chuẩn hoá phải nằm trong [0, 1].

SELECT
    observation_id,
    'window_not_ending_at_observation' AS violation
FROM {{ ref('fct_flood_training_feature') }}
WHERE forcing_hour_utc <> DATE_TRUNC('hour', observed_at_utc)

UNION ALL

SELECT
    f.observation_id,
    'historical_frequency_leaks_own_event' AS violation
FROM {{ ref('fct_flood_training_feature') }} f
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('fct_flood_event_observation') }} o
    WHERE o.location_key = f.location_key
      AND o.event_id = f.event_id
      AND o.observation_id <> f.observation_id
      AND o.is_flooded
      AND f.prior_flood_count > 0
      AND f.prior_observation_count = (
          SELECT COUNT(*)
          FROM {{ ref('fct_flood_event_observation') }} c
          WHERE c.location_key = f.location_key
            AND c.observed_at_utc < f.observed_at_utc
            AND c.is_active
      )
)

UNION ALL

SELECT
    observation_id,
    'ratio_out_of_unit_range' AS violation
FROM {{ ref('fct_flood_training_feature') }}
WHERE soil_moisture_index NOT BETWEEN 0 AND 1
   OR historical_flood_frequency NOT BETWEEN 0 AND 1
   OR historical_impassable_rate NOT BETWEEN 0 AND 1
