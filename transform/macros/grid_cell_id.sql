{% macro grid_cell_id(weather_model, weather_product, latitude, longitude) %}
MD5(CONCAT_WS(
    '|', {{ weather_model }}, {{ weather_product }},
    PRINTF('%.6f', {{ latitude }}), PRINTF('%.6f', {{ longitude }})
))
{% endmacro %}
