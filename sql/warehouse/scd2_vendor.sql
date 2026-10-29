-- STATUS: ILLUSTRATIVE ONLY — NOT PRODUCTION-READY.
-- Do not execute against the live mart_ds.dim_vendor table.
-- The current source derives placeholder descriptions from vendor_id,
-- so it cannot represent genuine vendor attribute changes.
-- Production SCD Type 2 requires a source with real, changeable attributes
-- and a defined effective-date strategy.
-- The synthetic SCD2 test is separate from this script.
--
-- Ride-Hailing Analytics: SCD Type 2 vendor dimension
-- Vendor descriptions are neutral placeholders, not official names.
-- Historical changes must come from a genuine source change.

DECLARE effective_date DATE DEFAULT CURRENT_DATE();

BEGIN TRANSACTION;

-- Step 1: Expire current records when source attributes change.
UPDATE `ride-hailing-analytics-512806.mart_ds.dim_vendor` AS target
SET
  effective_to = DATE_SUB(effective_date, INTERVAL 1 DAY),
  is_current = FALSE
WHERE target.is_current = TRUE
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT DISTINCT
        vendor_id AS vendor_key,
        TO_HEX(SHA256(CONCAT('Taxi vendor ', vendor_id))) AS record_hash
      FROM `ride-hailing-analytics-512806.mart_ds.fact_trips`
      WHERE vendor_id IS NOT NULL
    ) AS source
    WHERE source.vendor_key = target.vendor_key
      AND source.record_hash != target.record_hash
  );

-- Step 2: Insert vendors without a matching current version.
INSERT INTO `ride-hailing-analytics-512806.mart_ds.dim_vendor`
  (
    vendor_key,
    vendor_description,
    effective_from,
    effective_to,
    is_current,
    record_hash
  )
SELECT
  source.vendor_key,
  source.vendor_description,
  effective_date,
  DATE '9999-12-31',
  TRUE,
  source.record_hash
FROM (
  SELECT DISTINCT
    vendor_id AS vendor_key,
    CONCAT('Taxi vendor ', vendor_id) AS vendor_description,
    TO_HEX(SHA256(CONCAT('Taxi vendor ', vendor_id))) AS record_hash
  FROM `ride-hailing-analytics-512806.mart_ds.fact_trips`
  WHERE vendor_id IS NOT NULL
) AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM `ride-hailing-analytics-512806.mart_ds.dim_vendor` AS target
  WHERE target.vendor_key = source.vendor_key
    AND target.is_current = TRUE
    AND target.record_hash = source.record_hash
);

COMMIT TRANSACTION;
