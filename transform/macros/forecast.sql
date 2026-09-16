{% macro forecast_run_order() -%}
MAX(
    COALESCE(
        forecast_run_at,
        TRY_STRPTIME(
            REGEXP_EXTRACT(forecast_run_id, 'run_([0-9]{8}T[0-9]{6})', 1),
            '%Y%m%dT%H%M%S'
        ) AT TIME ZONE 'UTC'
    )
) DESC,
MAX(_ingested_at) DESC,
forecast_run_id DESC
{%- endmacro %}
