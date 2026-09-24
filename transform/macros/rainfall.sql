{# Shared rainfall windows and business bands. #}
{% macro rain_windows() %}
    {{ return([1, 3, 6, 12, 24]) }}
{% endmacro %}


{# RANGE giữ đúng time window khi nguồn thiếu giờ. #}
{% macro rolling_rain_sums(windows, partition_by, order_by='valid_at') %}
    {%- for hours in windows %}
    SUM(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
    ) AS rain_{{ hours }}h_sum_raw,
    COUNT(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN INTERVAL '{{ hours - 1 }} hours' PRECEDING AND CURRENT ROW
    ) AS rain_{{ hours }}h_hours{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{# NULL khi window thiếu giờ. #}
{% macro rolling_rain_columns(windows) %}
    {%- for hours in windows %}
    CASE WHEN rain_{{ hours }}h_hours = {{ hours }}
         THEN rain_{{ hours }}h_sum_raw END AS rain_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{# Forward window gồm current hour và H-1 giờ kế tiếp. #}
{% macro forward_rain_sums(windows, partition_by, order_by='valid_at') %}
    {%- for hours in windows %}
    SUM(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN CURRENT ROW AND INTERVAL '{{ hours - 1 }} hours' FOLLOWING
    ) AS forecast_next_{{ hours }}h_sum_raw,
    COUNT(precipitation_mm) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        RANGE BETWEEN CURRENT ROW AND INTERVAL '{{ hours - 1 }} hours' FOLLOWING
    ) AS forecast_next_{{ hours }}h_hours{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{% macro forward_rain_columns(windows) %}
    {%- for hours in windows %}
    CASE WHEN forecast_next_{{ hours }}h_hours = {{ hours }}
         THEN forecast_next_{{ hours }}h_sum_raw END
         AS forecast_next_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


{# Ngưỡng nghiệp vụ dùng chung cho mart và dashboard. #}
{% macro hanoi_rain_scenario_band(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 100 THEN 'over_100'
        WHEN {{ column }} >= 70 THEN 'from_70_to_100'
        WHEN {{ column }} >= 50 THEN 'from_50_to_under_70'
        ELSE 'below_50'
    END
{% endmacro %}


{% macro hanoi_rain_scenario_level(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 100 THEN 3
        WHEN {{ column }} >= 70 THEN 2
        WHEN {{ column }} >= 50 THEN 1
        ELSE 0
    END
{% endmacro %}


{% macro vn_rain_band_12h(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 100 THEN 'over_100'
        WHEN {{ column }} >= 70 THEN 'from_70_to_100'
        WHEN {{ column }} >= 50 THEN 'from_50_to_under_70'
        WHEN {{ column }} >= 30 THEN 'from_30_to_under_50'
        ELSE 'below_30'
    END
{% endmacro %}


{% macro vn_rain_band_24h(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        WHEN {{ column }} > 300 THEN 'over_300'
        WHEN {{ column }} > 200 THEN 'from_200_to_300'
        WHEN {{ column }} >= 150 THEN 'from_150_to_under_200'
        WHEN {{ column }} >= 100 THEN 'from_100_to_under_150'
        WHEN {{ column }} >= 50 THEN 'from_50_to_under_100'
        ELSE 'below_50'
    END
{% endmacro %}
