SELECT fact.grid_cell_id
FROM {{ ref('fct_rainfall_historical_hourly') }} AS fact
LEFT JOIN {{ ref('dim_grid') }} AS dim USING (grid_cell_id)
WHERE dim.grid_cell_id IS NULL
UNION ALL
SELECT fact.grid_cell_id
FROM {{ ref('fct_rainfall_forecast_hourly') }} AS fact
LEFT JOIN {{ ref('dim_grid') }} AS dim USING (grid_cell_id)
WHERE dim.grid_cell_id IS NULL
