SELECT *
FROM {{ ref('fct_rainfall_historical_hourly') }}
WHERE
    {% for hours in [1, 3, 6, 12, 24, 48, 72] %}
    NOT (
        (rain_{{ hours }}h_is_complete
         AND rain_{{ hours }}h_mm IS NOT NULL
         AND rain_{{ hours }}h_coverage_ratio = 1.0)
        OR
        (NOT rain_{{ hours }}h_is_complete
         AND rain_{{ hours }}h_mm IS NULL
         AND rain_{{ hours }}h_coverage_ratio >= 0.0
         AND rain_{{ hours }}h_coverage_ratio < 1.0)
    ){% if not loop.last %} OR{% endif %}
    {% endfor %}
