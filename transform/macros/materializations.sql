{# DuckLake table materialization: write directly to target path, keep dbt lifecycle. #}

{% materialization table, adapter="duckdb", supported_languages=['sql'] %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation   = this.incorporate(type='table') -%}
  {%- set grant_config      = config.get('grants') -%}
  {%- set use_ducklake_table_workarounds = adapter.use_ducklake_table_workarounds(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    CREATE OR REPLACE TABLE {{ target_relation }} AS (
      {{ compiled_code }}
    )
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% if not use_ducklake_table_workarounds %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  {{ adapter.commit() }}

  {% if use_ducklake_table_workarounds %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% materialization incremental, adapter="duckdb", supported_languages=['sql'] %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation   = this.incorporate(type='table') -%}
  {%- set grant_config      = config.get('grants') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {# false khi process chưa đủ checkpoint để chạy incremental. #}
  {%- set incremental_enabled = var('processing_incremental', false) -%}
  {%- set use_ducklake_table_workarounds = adapter.use_ducklake_table_workarounds(target_relation) -%}
  {%- set full_refresh = should_full_refresh() or existing_relation is none or not incremental_enabled -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if full_refresh %}
    {% call statement('main') -%}
      CREATE OR REPLACE TABLE {{ target_relation }} AS (
        {{ compiled_code }}
      )
    {%- endcall %}
  {% else %}
    {% if not unique_key %}
      {{ exceptions.raise_compiler_error("DuckLake incremental requires unique_key") }}
    {% endif %}
    {% call statement('create_slice') -%}
      CREATE OR REPLACE TEMP TABLE gold_inc_slice AS (
        {{ compiled_code }}
      )
    {%- endcall %}
    {% call statement('delete_keys') -%}
      DELETE FROM {{ target_relation }}
      WHERE {{ unique_key }} IN (SELECT {{ unique_key }} FROM gold_inc_slice)
    {%- endcall %}
    {% call statement('insert_slice') -%}
      INSERT INTO {{ target_relation }} SELECT * FROM gold_inc_slice
    {%- endcall %}
    {% call statement('main') -%}
      DROP TABLE gold_inc_slice
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=full_refresh) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% if not use_ducklake_table_workarounds %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  {{ adapter.commit() }}

  {% if use_ducklake_table_workarounds %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{# Giữ is_incremental() đồng bộ với custom materialization. #}
{% macro is_incremental() %}
    {#-- Không chạy introspective query lúc parse. #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(
            relation is not none
            and relation.type == 'table'
            and model.config.materialized in ('incremental', 'incremental_microbatch')
            and not should_full_refresh()
            and var('processing_incremental', false)
        ) }}
    {% endif %}
{% endmacro %}
