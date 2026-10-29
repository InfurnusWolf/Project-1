# Ride-Hailing Analytics — Architecture and Design

## 1. Project Overview

This solo capstone builds a cloud-based analytics pipeline for NYC Yellow Taxi
trip data. It processes historical Parquet data, creates curated datasets,
builds analytical warehouse tables and views, and presents results in a dashboard.

## 2. Architecture

1. **Source:** NYC Yellow Taxi historical Parquet data.
2. **Raw storage:** Google Cloud Storage bucket
   `gs://ride-hailing-analytics-512806-raw/`.
3. **Raw warehouse:** BigQuery dataset `raw_ds`, including the external table
   `yellow_tripdata_2022_03`.
4. **Transformation:** Dataproc Serverless with PySpark.
5. **Curated storage:** Google Cloud Storage bucket
   `gs://ride-hailing-analytics-512806-curated/rides/`, partitioned by pickup date.
6. **Warehouse:** BigQuery datasets `curated_ds` and `mart_ds`, with analytical
   tables, dimensions, facts, and views to be verified.
7. **Orchestration:** Cloud Composer/Airflow DAG named `ride_hailing_daily`,
   to be verified.
8. **Analytics:** BigQuery ML metrics and Looker Studio dashboard.

## 3. Data Layers

- **Raw:** Source records preserved in GCS and exposed through BigQuery.
- **Curated:** Cleaned and typed Parquet data, deduplicated and partitioned by
  `pickup_date`.
- **Mart:** Business-facing aggregates and views for trip volume, revenue,
  demand by hour, and pickup-zone analysis.

## 4. Transformation Rules

The PySpark job standardizes column names and types, filters invalid timestamps
and trip values, removes duplicate records, derives trip duration, pickup date,
pickup hour, average speed and tip percentage, and writes partitioned Parquet.

## 5. Cloud Resources

- GCP project: `ride-hailing-analytics-512806`
- Region: `us-east4`
- GCS buckets: raw, curated, and staging
- BigQuery datasets: `raw_ds`, `curated_ds`, `mart_ds`
- Dataproc Serverless: PySpark batch processing
- Pub/Sub, Dataflow, and Cloud Composer: ingestion/orchestration components
  whose working implementations must be verified.

## 6. Current Validation Evidence

- Raw external table created: `raw_ds.yellow_tripdata_2022_03`
- Source row count: 3,627,882
- Null pickup timestamps: 0
- Null drop-off timestamps: 0
- Spark script: `spark/clean_rides.py`
- Python syntax validation passed.

## 7. Outstanding Verification

The streaming ingestion pipeline, fact/dimension tables, BQML model and metrics,
`ride_hailing_daily` DAG, dashboard URL, cost report, demo recording, and rubric
must be verified or completed before final submission.
