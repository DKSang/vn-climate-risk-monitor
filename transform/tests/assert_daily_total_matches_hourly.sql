-- fct_rain_archive_daily là tổng của fct_rain_archive_hourly. Nếu lệch thì hoặc GROUP BY sai
-- hoặc incremental của bảng giờ để lại dòng mồ côi. Ngưỡng 0,01mm cho sai số
-- dấu phẩy động khi cộng ~24 giá trị DOUBLE.
WITH expected AS (
    SELECT grid_cell_id, rain_date, SUM(precipitation_mm) AS total_mm
    FROM {{ ref('fct_rain_archive_hourly') }}
    GROUP BY grid_cell_id, rain_date
)
SELECT d.grid_cell_id, d.rain_date, d.rain_total_mm, e.total_mm
FROM {{ ref('fct_rain_archive_daily') }} AS d
FULL OUTER JOIN expected AS e
    ON e.grid_cell_id = d.grid_cell_id AND e.rain_date = d.rain_date
WHERE d.grid_cell_id IS NULL
   OR e.grid_cell_id IS NULL
   OR ABS(COALESCE(d.rain_total_mm, 0) - COALESCE(e.total_mm, 0)) > 0.01
