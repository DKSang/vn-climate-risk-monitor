/*
    Test chống "bảng ma" (phantom table).

    Bối cảnh: 2026-08-20 phát hiện bảng gold có metadata trong catalog Postgres
    nhưng file Parquet không tồn tại trên MinIO. Khi đó:
      - SELECT COUNT(*)  -> trả đúng số dòng (đọc từ thống kê metadata)
      - test not_null    -> PASS  <-- GREEN GIẢ
      - SELECT *         -> HTTP 404 NoSuchKey

    Test này dùng COUNT(DISTINCT ...) trên cột VARCHAR để buộc engine đọc dữ liệu
    thật từ Parquet, không thể trả lời bằng thống kê. Nếu file không tồn tại,
    test ERROR thay vì PASS nhầm.

    Đồng thời khẳng định đúng 126 phường/xã Hà Nội (NQ 1656/NQ-UBTVQH15).
*/

SELECT
    COUNT(*)                    AS so_dong,
    COUNT(DISTINCT ward_code)   AS so_ma_duy_nhat,
    COUNT(DISTINCT ward_name)   AS so_ten_duy_nhat
FROM {{ ref('dim_hanoi_ward') }}
HAVING
    COUNT(*) <> 126
    OR COUNT(DISTINCT ward_code) <> 126
