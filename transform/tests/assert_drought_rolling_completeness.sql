SELECT *
FROM {{ ref('fct_rainfall_drought_daily') }}
WHERE
    {% for days in [30, 60, 90] %}
    NOT (
        (rain_{{ days }}d_is_complete
         AND rain_{{ days }}d_mm IS NOT NULL
         AND rain_{{ days }}d_coverage_ratio = 1.0)
        OR
        (NOT rain_{{ days }}d_is_complete
         AND rain_{{ days }}d_mm IS NULL
         AND rain_{{ days }}d_coverage_ratio >= 0.0
         AND rain_{{ days }}d_coverage_ratio < 1.0)
    ){% if not loop.last %} OR{% endif %}
    {% endfor %}
