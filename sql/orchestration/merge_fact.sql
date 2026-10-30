MERGE `ride-hailing-analytics-512806.curated_ds.fact_rides` AS T
USING (
  SELECT
    ride_id,
    driver_id,
    rider_id,
    pickup_zone,
    dropoff_zone,
    pickup_ts,
    dropoff_ts,
    DATE(pickup_ts) AS pickup_date,
    EXTRACT(HOUR FROM pickup_ts) AS pickup_hour,
    fare,
    surge_multiplier,
    vendor_id,
    trip_distance,
    passenger_count,
    trip_duration_minutes,
    SAFE_DIVIDE(fare, trip_distance) AS revenue_per_mile,
    COALESCE(surge_multiplier > 1, FALSE) AS surge_flag,
    ingest_ts,
    source_type
  FROM `ride-hailing-analytics-512806.curated_ds.v_unified_rides`
  WHERE pickup_ts IS NOT NULL
    AND dropoff_ts IS NOT NULL
    AND fare >= 0
    AND TIMESTAMP_DIFF(dropoff_ts, pickup_ts, SECOND) > 0
    AND TIMESTAMP_DIFF(dropoff_ts, pickup_ts, SECOND) <= 14400
) AS S
ON (
  T.source_type = S.source_type
  AND (
    (
      S.source_type = 'streaming'
      AND T.ride_id = S.ride_id
    )
    OR
    (
      S.source_type = 'historical'
      AND T.vendor_id = S.vendor_id
      AND T.pickup_ts = S.pickup_ts
      AND T.dropoff_ts = S.dropoff_ts
      AND T.pickup_zone = S.pickup_zone
      AND T.dropoff_zone = S.dropoff_zone
      AND T.fare = S.fare
      AND T.trip_distance = S.trip_distance
      AND T.passenger_count = S.passenger_count
    )
  )
)
WHEN NOT MATCHED THEN
  INSERT (
    ride_id,
    driver_id,
    rider_id,
    pickup_zone,
    dropoff_zone,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    pickup_hour,
    fare,
    surge_multiplier,
    vendor_id,
    trip_distance,
    passenger_count,
    trip_duration_minutes,
    revenue_per_mile,
    surge_flag,
    ingest_ts,
    source_type
  )
  VALUES (
    S.ride_id,
    S.driver_id,
    S.rider_id,
    S.pickup_zone,
    S.dropoff_zone,
    S.pickup_ts,
    S.dropoff_ts,
    S.pickup_date,
    S.pickup_hour,
    S.fare,
    S.surge_multiplier,
    S.vendor_id,
    S.trip_distance,
    S.passenger_count,
    S.trip_duration_minutes,
    S.revenue_per_mile,
    S.surge_flag,
    S.ingest_ts,
    S.source_type
  );
