{% test not_empty(model) %}

select 1 as failure
where not exists (select 1 from {{ model }})

{% endtest %}

{% test accepted_range(model, column_name, min_value, max_value) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}

{% test unique_combination(model, columns) %}

select {{ columns | join(', ') }}
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}

{# Wider rainfall windows must not total less than narrower ones. #}
{% test rain_windows_monotonic(model) %}
{% set windows = rain_windows() %}
select *
from {{ model }}
where false
{%- for hours in windows %}{% if not loop.first %}
   or rain_{{ hours }}h_mm < rain_{{ windows[loop.index0 - 1] }}h_mm
{%- endif %}{% endfor %}
{% endtest %}
