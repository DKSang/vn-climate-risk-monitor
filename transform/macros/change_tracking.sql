{% macro stable_row_hash(columns) %}
MD5(CONCAT_WS('|',
    {%- for column in columns %}
    COALESCE(CAST({{ column }} AS VARCHAR), ''){{ "," if not loop.last }}
    {%- endfor %}
))
{% endmacro %}


{% macro incremental_new_or_changed(key, alias='incoming') %}
{% if is_incremental() %}
LEFT JOIN {{ this }} AS existing
    ON existing.{{ key }} = {{ alias }}.{{ key }}
WHERE existing.{{ key }} IS NULL
   OR existing._row_hash <> {{ alias }}._row_hash
{% endif %}
{% endmacro %}
