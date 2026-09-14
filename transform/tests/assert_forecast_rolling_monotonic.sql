-- Wider rainfall windows must not total less than narrower ones.
{% set windows = rain_windows() %}

SELECT
    rain_forecast_hourly_key,
    {%- for hours in windows %}
    rain_{{ hours }}h_mm{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ ref('fct_rain_forecast_hourly') }}
WHERE FALSE
    {%- for hours in windows %}
    {%- if not loop.first %}
    OR rain_{{ hours }}h_mm < rain_{{ windows[loop.index0 - 1] }}h_mm
    {%- endif %}
    {%- endfor %}
