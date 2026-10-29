# Ride-Hailing Analytics Pipeline

An end-to-end data engineering capstone that processes NYC taxi trip data using Google Cloud Platform, Apache Spark, BigQuery, and Cloud Composer.

## Project Overview

This project demonstrates a data pipeline for ingesting, cleaning, validating, and analyzing ride-hailing trip data. It uses a layered data architecture and includes automated data quality checks.

## Architecture

The pipeline includes:

1. **Raw layer:** Source taxi trip data stored in Google Cloud Storage.
2. **Processing layer:** PySpark cleaning and deduplication using Dataproc Serverless.
3. **Curated layer:** Processed ride data stored in Parquet and exposed through BigQuery.
4. **Analytics layer:** BigQuery views for ride and revenue analysis.
5. **Orchestration and validation:** Cloud Composer DAG with BigQuery data quality checks.

See [`docs/architecture/architecture.png`](docs/architecture/architecture.png) for the architecture diagram.

## Technology Stack

- Google Cloud Storage
- Dataproc Serverless
- Apache Spark / PySpark
- BigQuery
- Cloud Composer / Apache Airflow
- Python
- SQL
- Git and GitHub

## Repository Structure

```text
.
├── dags/
│   └── ride_hailing_daily.py
├── data/
│   └── reference/
│       └── taxi_zone_lookup.csv
├── docs/
│   ├── architecture/
│   ├── design.md
│   └── ingestion.md
├── reports/
│   └── airflow_quality_checks.md
├── spark/
│   └── clean_rides.py
└── sql/
    └── warehouse/
        └── scd2_vendor.sql
```

## Implemented Components

- PySpark cleaning and deduplication job.
- Partitioned Parquet output by pickup date.
- BigQuery curated and analytics views.
- Cloud Composer DAG with four data quality checks.
- SQL example demonstrating Slowly Changing Dimension Type 2 (SCD2).
- Architecture, design, ingestion, and validation documentation.

## Data Quality Validation

The `ride_hailing_daily` Airflow DAG validates:

- The curated table contains records.
- Pickup and drop-off timestamps are not null.
- Trip distance is positive.
- Fare amount is non-negative.

All four checks passed in the documented manual DAG run. See [`reports/airflow_quality_checks.md`](reports/airflow_quality_checks.md) for the recorded results.

## Running and Verification

The Spark job and Airflow DAG depend on the configured GCP project, service accounts, bucket paths, BigQuery tables, and Composer environment. Configure these resources before running the pipeline.

Review the scripts and documentation in `spark/`, `dags/`, `docs/`, and `sql/` for implementation details.

## Notes

- The SCD2 SQL file is an example and should be reviewed before execution against live tables.
- Cloud resources may incur charges while running.
