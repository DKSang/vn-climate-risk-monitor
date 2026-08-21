/*
    Bảo vệ zero-padding của mã hành chính trong seed.

    Bối cảnh: CSV gốc lưu province_code = '01' và commune_code = '00004'.
    Khi còn đọc bằng read_csv_auto() trên MinIO, dbt/DuckDB suy kiểu thành BIGINT
    -> '01' thành 1, '00004' thành 4, MẤT số 0 đầu. Silver phải LPAD lại để join
    được với danh mục GSO (vốn lưu VARCHAR đã pad).

    Sau khi chuyển sang seed, `column_types` trong dbt_project.yml ép VARCHAR nên
    padding được giữ. Test này chặn hồi quy: nếu ai đó bỏ column_types, hoặc thêm
    dòng có mã sai độ dài, build sẽ fail thay vì âm thầm sinh join hỏng.

    Fail khi: có bất kỳ dòng nào province_code khác 2 ký tự hoặc commune_code
    khác 5 ký tự.
*/

SELECT
    location_key,
    province_code,
    commune_code,
    LENGTH(province_code)  AS len_province_code,
    LENGTH(commune_code)   AS len_commune_code
FROM {{ ref('ward_coordinates_seed') }}
WHERE LENGTH(province_code) <> 2
   OR LENGTH(commune_code)  <> 5
