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
