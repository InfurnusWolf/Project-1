SELECT
  COUNT(*) AS total_records,
  COUNTIF(is_current) AS current_records,
  COUNTIF(driver_id IS NULL) AS null_driver_ids,
  COUNTIF(effective_start IS NULL) AS null_effective_start,
  COUNTIF(
    effective_end IS NOT NULL
    AND effective_end <= effective_start
  ) AS invalid_date_ranges
FROM `ride-hailing-analytics-512806.curated_ds.dim_driver_scd2`;
