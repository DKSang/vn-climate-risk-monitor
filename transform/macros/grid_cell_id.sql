{#
    Định danh ô lưới. Ô lưới thuộc về MODEL, không thuộc về endpoint:
    era5 (0,25°) và ecmwf_ifs (~9km) có lưới khác nhau, còn cùng một model thì
    archive và forecast trả cùng toạ độ.

    Bản cũ có thêm tham số `weather_product` — nó chỉ nhân đôi id cho cùng một ô
    vật lý, khiến fact archive và fact forecast không join được với nhau qua
    `dim_grid`.

    Toạ độ PHẢI đã round 6 số trước khi vào đây (làm ở Silver, đúng một chỗ).
#}
{% macro grid_cell_id(weather_model, latitude, longitude) %}
MD5(CONCAT_WS(
    '|', {{ weather_model }},
    PRINTF('%.6f', {{ latitude }}), PRINTF('%.6f', {{ longitude }})
))
{% endmacro %}
