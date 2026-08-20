{#
    Materialization `table` cho DuckLake — ghi thẳng vào tên bảng đích.

    ── VÌ SAO CẦN ──────────────────────────────────────────────────────────────
    Materialization mặc định của dbt-duckdb dùng chiến lược create-then-swap:

        1. CREATE TABLE <model>__dbt_tmp AS (...)
        2. ALTER TABLE <model>        RENAME TO <model>__dbt_backup
        3. ALTER TABLE <model>__dbt_tmp RENAME TO <model>
        4. DROP TABLE <model>__dbt_backup

    Với DuckLake, đường dẫn vật lý của file Parquet được quyết định ở BƯỚC 1 theo
    tên bảng lúc tạo. Bước 3 chỉ đổi tên trong catalog Postgres, KHÔNG di dời file
    trên object storage. Kết quả: dữ liệu của bảng `wards_raw` nằm vĩnh viễn ở
        s3://vn-climate/bronze/wards_raw__dbt_tmp/ducklake-*.parquet

    Đã thử và loại các cách khác (2026-08-20):
      - ducklake_rewrite_data_files()   -> chạy OK nhưng KHÔNG đổi đường dẫn
      - ducklake_merge_adjacent_files() -> tương tự, không đổi
      - dbt-duckdb có adapter.use_ducklake_table_workarounds() nhưng chỉ xử lý
        vấn đề persist_docs ở DuckLake < 1.5.3, không liên quan đường dẫn.

    ── KHÁC GÌ BẢN CŨ ĐÃ LÀM HỎNG DỰ ÁN ────────────────────────────────────────
    Bản macro trước đây cũng ghi thẳng CREATE OR REPLACE TABLE, nhưng BỎ QUA
    adapter.commit() và toàn bộ hooks. Hậu quả đo được:
      - DuckLake ghi metadata vào Postgres nhưng Parquet không được finalize
      - Bảng "ma": SELECT COUNT(*) trả 126 (từ metadata), SELECT * lỗi HTTP 404
      - dbt test cho green giả (not_null PASS vì đọc từ thống kê, không đọc file)

    Bản này giữ ĐẦY ĐỦ vòng đời của materialization chuẩn — pre/post hooks,
    grants, persist_docs và quan trọng nhất là adapter.commit() — chỉ thay
    create-then-swap bằng CREATE OR REPLACE tại đúng tên đích.

    Chỉ hỗ trợ SQL. Model Python sẽ báo lỗi rõ ràng thay vì hỏng ngầm.
#}

{% materialization table, adapter="duckdb", supported_languages=['sql'] %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation   = this.incorporate(type='table') -%}
  {%- set grant_config      = config.get('grants') -%}
  {%- set use_ducklake_table_workarounds = adapter.use_ducklake_table_workarounds(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` xảy ra ở đây
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Ghi thẳng vào tên đích -> file Parquet nằm đúng s3://.../<schema>/<table>/
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

  -- `COMMIT` xảy ra ở đây — ĐÂY LÀ THỨ BẢN CŨ THIẾU
  {{ adapter.commit() }}

  {% if use_ducklake_table_workarounds %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
