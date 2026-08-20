{{ config(materialized = 'view') }}

/*
    SILVER — bản đã validate và JOIN, CHƯA aggregate.

    Đúng mẫu `customer_transactions` trong ví dụ của Microsoft: hai nguồn silver
    đã làm sạch được join thành một thực thể nghiệp vụ, vẫn giữ độ chi tiết đầy đủ.
    Microsoft liệt kê `Joins` là thao tác hợp lệ của lớp silver.

    Đây là "ward master" toàn quốc — 3.321 phường/xã, chưa lọc tỉnh nào.
    Gold sẽ lọc phạm vi và dựng chiều từ đây. Bảng này cũng sẽ được dùng lại khi
    ánh xạ phường/xã sang ô lưới Open-Meteo.

    Chất lượng dữ liệu (kiểm chứng 2026-08-20):
      - Hà Nội khớp đủ 126/126 giữa hai nguồn.
      - Toàn quốc lệch 2 dòng do mã khác nhau giữa 2 nguồn:
          Xã Ba Chẽ (Quảng Ninh) 06970 vs 06978
          Xã Ia Mơ  (Gia Lai)    23737 vs 23938
        Dùng LEFT JOIN từ phía toạ độ nên không mất dòng; cột GSO sẽ NULL ở 2 dòng đó.
        Cột has_gso_match cho phép downstream phát hiện và xử lý.
*/

SELECT
    c.location_key,
    c.province_code,
    c.province_name,
    c.ward_code,
    c.ward_name,
    w.ward_name_en,
    w.ward_full_name,
    w.ward_slug,
    w.administrative_unit_id,
    c.latitude,
    c.longitude,
    c.region,
    c.climate_zone,
    (w.ward_code IS NOT NULL)   AS has_gso_match,
    c.bronze_ingested_at
FROM {{ ref('ward_coordinates_cleaned') }} c
LEFT JOIN {{ ref('wards_cleaned') }} w
    ON c.ward_code = w.ward_code
