SELECT
  source_type,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT IF(
    source_type = 'streaming',
    ride_id,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      vendor_id, pickup_ts, dropoff_ts, pickup_zone, dropoff_zone,
      fare, trip_distance, passenger_count
    )))
  )) AS distinct_keys
FROM `ride-hailing-analytics-512806.curated_ds.v_unified_rides`
WHERE pickup_ts IS NOT NULL
  AND dropoff_ts IS NOT NULL
  AND fare >= 0
  AND TIMESTAMP_DIFF(dropoff_ts, pickup_ts, SECOND) BETWEEN 1 AND 14400
GROUP BY source_type;
