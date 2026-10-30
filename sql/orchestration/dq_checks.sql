WITH checks AS (
  SELECT
    COUNT(*) AS total_rows,
    COUNTIF(pickup_ts IS NULL) AS null_pickup_ts,
    COUNTIF(dropoff_ts IS NULL) AS null_dropoff_ts,
    COUNTIF(fare IS NULL OR fare < 0) AS invalid_fares,
    COUNTIF(
      trip_duration_minutes IS NULL
      OR trip_duration_minutes <= 0
      OR trip_duration_minutes > 240
    ) AS invalid_durations,
    COUNTIF(source_type = 'streaming' AND ride_id IS NULL)
      AS streaming_null_ids
  FROM `ride-hailing-analytics-512806.curated_ds.fact_rides`
)
SELECT
  *,
  total_rows > 0
    AND null_pickup_ts = 0
    AND null_dropoff_ts = 0
    AND invalid_fares = 0
    AND invalid_durations = 0
    AND streaming_null_ids = 0 AS dq_passed
FROM checks;
