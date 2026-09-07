/*
    STAGING — phường × model thời tiết → ô lưới.

    KHÔNG tính nearest-neighbour. Ánh xạ này lấy từ CHÍNH phép snap của
    Open-Meteo (`make map-grid` ghi lại toạ độ ô mà API trả về cho từng phường);
    nearest-neighbour tự tính ra kết quả LỆCH ở các ô biên. Bronze không giữ
    được thông tin này vì response không mang lại mã phường đã yêu cầu.

    Đây cũng là thứ cho phép fetch theo Ô thay vì theo PHƯỜNG: 126 phường Hà Nội
    chỉ rơi vào 12 ô era5 / 48 ô ecmwf_ifs, nên fetch theo phường tốn quota gấp
    ~10 lần mà không thêm một thông tin nào.
*/

{{ config(materialized = 'view') }}

SELECT
    -- DuckDB suy luận cột CSV này là số nếu không ép kiểu; LPAD giữ mã hành
    -- chính 5 ký tự để join đúng với dim_ward (ví dụ 00004, không phải 4).
    LPAD(CAST(ward_code AS VARCHAR), 5, '0') AS ward_code,
    model AS weather_model,
    {{ grid_cell_id(
        'model',
        'ROUND(grid_latitude, 6)',
        'ROUND(grid_longitude, 6)'
    ) }} AS grid_cell_id,
    ROUND(grid_latitude, 6) AS grid_latitude,
    ROUND(grid_longitude, 6) AS grid_longitude,
    elevation_m AS grid_elevation_m,
    TRUE AS is_active
FROM {{ ref('ward_grid_map_seed') }}
