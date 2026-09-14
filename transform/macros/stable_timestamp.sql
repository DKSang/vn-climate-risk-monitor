{% macro stable_timestamp(value) %}
CAST(EPOCH_US({{ value }}) AS VARCHAR)
{% endmacro %}
