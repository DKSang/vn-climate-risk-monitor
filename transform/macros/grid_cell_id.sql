{# Grid identity = model + canonical 6-decimal coordinates. #}
{% macro grid_cell_id(weather_model, latitude, longitude) %}
MD5(CONCAT_WS(
    '|', {{ weather_model }},
    PRINTF('%.6f', {{ latitude }}), PRINTF('%.6f', {{ longitude }})
))
{% endmacro %}
