{% macro stable_row_hash(columns) %}
MD5(CONCAT_WS('|',
    {%- for column in columns %}
    COALESCE(CAST({{ column }} AS VARCHAR), ''){{ "," if not loop.last }}
    {%- endfor %}
))
{% endmacro %}
