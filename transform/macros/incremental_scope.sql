{# Incremental read/write scope derived from processing checkpoints. #}

{# Cho hop 1:1 không cần mở rộng window. #}
{% macro incremental_changed_filter(
    source_ref, change_column='_ingested_at', prefix='WHERE'
) %}
{%- set lower = var('processing_bounds', {}).get(source_ref) -%}
{%- if lower and is_incremental() -%}
{{ prefix }} {{ change_column }} > TIMESTAMPTZ '{{ lower }}'
{%- endif -%}
{% endmacro %}


{%- macro _changed_rows(relation, source_ref, change_column) -%}
{%- set lower = var('processing_bounds', {}).get(source_ref) -%}
SELECT * FROM {{ relation }} WHERE {{ change_column }} > TIMESTAMPTZ '{{ lower }}'
{%- endmacro -%}


{# Rows cần đọc để tính lại output bị ảnh hưởng. #}
{% macro incremental_input_scope(
    relation,
    source_ref,
    dimension,
    expand_backward='0 hours',
    expand_forward='0 hours',
    keys=[],
    change_column='_ingested_at'
) %}
{%- set lower = var('processing_bounds', {}).get(source_ref) -%}
{%- if lower and is_incremental() -%}
{%- set changed = _changed_rows(relation, source_ref, change_column) -%}
WHERE {{ dimension }} >= (
        SELECT MIN({{ dimension }}) - INTERVAL '{{ expand_backward }}'
        FROM ({{ changed }}) AS changed
    )
  AND {{ dimension }} <= (
        SELECT MAX({{ dimension }}) + INTERVAL '{{ expand_forward }}'
        FROM ({{ changed }}) AS changed
    )
{%- for key in keys %}
  AND {{ key }} IN (
        SELECT DISTINCT {{ key }} FROM ({{ changed }}) AS changed
    )
{%- endfor %}
{%- endif -%}
{% endmacro %}


{# Chỉ emit output rows bị ảnh hưởng. #}
{% macro incremental_output_scope(
    relation,
    source_ref,
    dimension,
    expand_backward='0 hours',
    expand_forward='0 hours',
    change_column='_ingested_at'
) %}
{%- set lower = var('processing_bounds', {}).get(source_ref) -%}
{%- if lower and is_incremental() -%}
{%- set changed = _changed_rows(relation, source_ref, change_column) -%}
WHERE {{ dimension }} >= (
        SELECT MIN({{ dimension }})
        {%- if expand_backward != '0 hours' %} - INTERVAL '{{ expand_backward }}'{% endif %}
        FROM ({{ changed }}) AS changed
    )
  AND {{ dimension }} <= (
        SELECT MAX({{ dimension }}) + INTERVAL '{{ expand_forward }}'
        FROM ({{ changed }}) AS changed
    )
{%- endif -%}
{% endmacro %}


{# Production dùng control-plane run start; dbt chạy tay fallback local clock. #}
{% macro processing_updated_at() %}
{%- set run_started_at = var('processing_run_started_at', '') -%}
{%- if run_started_at -%}
TIMESTAMPTZ '{{ run_started_at }}'
{%- else -%}
CURRENT_TIMESTAMP
{%- endif -%}
{% endmacro %}
