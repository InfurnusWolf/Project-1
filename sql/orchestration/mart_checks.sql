SELECT
  'v_daily_revenue' AS view_name,
  COUNT(*) AS row_count
FROM `ride-hailing-analytics-512806.mart_ds.v_daily_revenue`

UNION ALL

SELECT
  'v_zone_demand_hourly',
  COUNT(*)
FROM `ride-hailing-analytics-512806.mart_ds.v_zone_demand_hourly`

UNION ALL

SELECT
  'v_driver_leaderboard',
  COUNT(*)
FROM `ride-hailing-analytics-512806.mart_ds.v_driver_leaderboard`;
