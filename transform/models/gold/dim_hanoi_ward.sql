{{ config(
    materialized = 'table',
    tags = ['gold', 'dimension', 'hanoi']
) }}

/*
    GOLD — Dimensional modeling (chuẩn Microsoft medallion).

    Nhiệm vụ lớp gold theo tài liệu Microsoft:
      "Dimensional modeling and aggregation"
      "often highly aggregated and filtered for specific time periods or
       geographic regions"
      "contains semantically meaningful datasets that map to business functions"

    Model này là **chiều** (dimension) cho toàn bộ phân tích rủi ro khí hậu:
    126 phường/xã Hà Nội sau sắp xếp 2025 (NQ 1656/NQ-UBTVQH15).

    Việc làm sạch và JOIN đã xong ở silver.ward_locations -> lớp này chỉ còn:
      - lọc phạm vi địa lý (Hà Nội)
      - đặt khoá chiều và thuộc tính nghiệp vụ

    Fact table tương lai (fct_rainfall_hourly, fct_flood_risk_hourly) sẽ join
    vào ward_key của bảng này.
*/

SELECT
    location_key            AS ward_key,
    ward_code,
    ward_name,
    COALESCE(ward_name_en, '')   AS ward_name_en,
    COALESCE(ward_slug, '')      AS ward_slug,
    province_code,
    province_name,
    latitude,
    longitude,
    region,
    climate_zone,
    bronze_ingested_at
FROM {{ ref('ward_locations') }}
WHERE province_code = '01'
ORDER BY location_key
