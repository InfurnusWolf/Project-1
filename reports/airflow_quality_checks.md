# Airflow Data Quality Validation

## Overview
- **DAG ID:** `ride_hailing_daily`
- **Environment:** Cloud Composer 3
- **GCP Region:** `us-east4`
- **BigQuery table:** `ride-hailing-analytics-512806.curated_ds.rides_external`
- **Run ID:** `manual__2026-10-29T09:26:18+00:00`

## Validation Results

| Task | Validation | Result |
|---|---|---|
| `validate_curated_data` | Curated table contains records | Success |
| `validate_timestamps` | Pickup and drop-off timestamps are not null | Success |
| `validate_trip_distance` | Trip distance is not null and greater than zero | Success |
| `validate_fare_amount` | Fare amount is not null and non-negative | Success |

## Summary
All four quality-check tasks completed successfully in the recorded DAG run.

These checks validate that the curated dataset contains records and meets the configured timestamp, distance, and fare rules.
