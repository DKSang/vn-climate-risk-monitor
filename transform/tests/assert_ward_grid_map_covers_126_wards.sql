-- Mỗi model archive phải ánh xạ ĐỦ 126 phường Hà Nội.
--
-- Seed sinh bởi `make map-grid` là ảnh chụp tại một thời điểm. Nếu danh sách
-- phường đổi (nghị quyết sắp xếp mới) mà quên chạy lại, phường mới sẽ lặng lẽ
-- biến mất khỏi mọi KPI: Bronze fetch theo ô nên nó vẫn có dữ liệu, chỉ là không
-- ai gắn nó vào phường nào. Test này bắt đúng ca đó.

SELECT
    weather_model,
    COUNT(DISTINCT ward_code) AS mapped_wards
FROM {{ ref('ward_grid_map') }}
GROUP BY 1
HAVING COUNT(DISTINCT ward_code) <> (SELECT COUNT(*) FROM {{ ref('dim_hanoi_ward') }})
