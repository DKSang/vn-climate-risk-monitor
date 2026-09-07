-- FAR/CSI/Brier không được giả là đo được trên tập nhãn chỉ có một lớp.
SELECT *
FROM {{ ref('fct_flood_backtest_metric') }}
WHERE negative_observation_count = 0
  AND (far IS NOT NULL OR csi IS NOT NULL OR brier_score IS NOT NULL)
