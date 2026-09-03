{#
    Cửa sổ đọc/ghi cho model incremental, sinh từ checkpoint của processing
    framework (`scripts/run_processing.py`).

    ── VÌ SAO CẦN MACRO ────────────────────────────────────────────────────────
    Model rolling window từng viết tay `INTERVAL '<n> hours'` ở BA chỗ. Đó không
    phải trùng lặp thẩm mỹ: nếu ba chỗ lệch nhau thì cửa sổ tính trên dữ liệu
    thiếu và ra số SAI mà mọi test not_null/unique vẫn PASS.

    ── VÌ SAO HAI MACRO, KHÔNG PHẢI MỘT ────────────────────────────────────────
    Input scope và output scope KHÔNG đối xứng.

    Một row mới ở giờ T làm sai mọi rolling window phủ T, tức các row đầu ra
    trong [T, T+71h]. Để TÍNH được các row đó thì phải ĐỌC từ T−71h:

                  đọc                          ghi
        ├───────────────────────────┤  ├──────────────────┤
      MIN−71h                     MIN                  MAX+71h
                                   └── row mới ──┘

        input  : [MIN(changed) − backward, MAX(changed) + forward]
        output : [MIN(changed)           , MAX(changed) + forward]

    Nới output xuống MIN−71h sẽ ghi đè những row hoàn toàn không bị ảnh hưởng.

    ── VÌ SAO LOOKBACK KHAI BÁO Ở MODEL, KHÔNG Ở YAML ─────────────────────────
    Một process build nhiều model có window khác nhau. Độ
    rộng window là thuộc tính của SQL sinh ra nó, nên phải version cùng file đó.
    Framework chỉ cấp chặn dưới qua var `processing_bounds`.

    ── KHÔNG CÓ UPPER BOUND ────────────────────────────────────────────────────
    Chặn trên theo run_start nghe có vẻ cho batch deterministic nhưng lại làm
    mất data: `_ingested_at` là transaction START time, row chỉ visible lúc
    COMMIT. Xem `safety_lag` trong `processing/*.yml`.
#}

{% macro processing_lower_bound(source_ref) %}
    {{- var('processing_bounds', {}).get(source_ref) -}}
{% endmacro %}


{#
    Bộ lọc incremental TỐI GIẢN: chỉ `change_column > lower_bound`.

    Dùng cho hop không có cửa sổ trượt — ví dụ staging → curated, nơi mỗi dòng
    đầu ra chỉ phụ thuộc chính dòng đầu vào của nó. Không cần nới scope theo
    `dimension` như `incremental_input_scope`, và cũng không nên: nới scope ở đây
    chỉ làm đọc thừa.

    `prefix` cho phép nối vào một WHERE đã có ('AND') thay vì mở WHERE mới.
#}
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


{#
    Rows cần ĐỌC để tính lại đúng. Trả chuỗi rỗng khi full refresh.

    `keys` thu hẹp thêm theo business key đã đổi — với dataset rộng thì đây là
    thứ quyết định incremental có tiết kiệm scan thật hay không.
#}
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


{#
    Rows thực sự BỊ ẢNH HƯỞNG, tức phần được MERGE vào bảng đích.
    Chặn dưới KHÔNG nới lùi: row trước MIN(changed) không bị dữ liệu mới đụng tới.
#}
{% macro incremental_output_scope(
    relation,
    source_ref,
    dimension,
    expand_forward='0 hours',
    change_column='_ingested_at'
) %}
{%- set lower = var('processing_bounds', {}).get(source_ref) -%}
{%- if lower and is_incremental() -%}
{%- set changed = _changed_rows(relation, source_ref, change_column) -%}
WHERE {{ dimension }} >= (
        SELECT MIN({{ dimension }}) FROM ({{ changed }}) AS changed
    )
  AND {{ dimension }} <= (
        SELECT MAX({{ dimension }}) + INTERVAL '{{ expand_forward }}'
        FROM ({{ changed }}) AS changed
    )
{%- endif -%}
{% endmacro %}


{#
    Dấu thời gian cho `_updated_at` của lớp mutable.

    Bình thường lấy `run_started_at` mà `scripts/run_processing.py` bơm xuống —
    cùng đồng hồ Postgres với `_ingested_at`, và sớm hơn lúc ghi thật nên
    watermark downstream không nhảy qua dòng vừa ghi.

    Chạy dbt TAY (không qua framework) thì không có var đó và ta rơi về
    CURRENT_TIMESTAMP của DuckDB. Chấp nhận được vì lần chạy tay KHÔNG advance
    checkpoint, và `safety_lag` 15 phút hấp thụ lệch đồng hồ ở mức máy đơn.
    Nhưng đó là đường phụ: pipeline production luôn đi qua `make transform`.
#}
{% macro processing_updated_at() %}
{%- set run_started_at = var('processing_run_started_at', '') -%}
{%- if run_started_at -%}
TIMESTAMPTZ '{{ run_started_at }}'
{%- else -%}
CURRENT_TIMESTAMP
{%- endif -%}
{% endmacro %}
