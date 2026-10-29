# Historical Data Ingestion

## Source
- GCS object: `gs://ride-hailing-analytics-512806-raw/yellow_tripdata_2022-03.parquet`
- Format: Parquet
- Period: March 2022 NYC Yellow Taxi data

## BigQuery destination
- Project: `ride-hailing-analytics-512806`
- Dataset: `raw_ds`
- Table: `yellow_tripdata_2022_03`
- Table type: External table
- Location: `us-east4`

## Validation results
- Total rows: 3,627,882
- Null pickup timestamps: 0
- Null drop-off timestamps: 0

## Implementation note
The external table reads the Parquet object directly from Google Cloud Storage.
This validates historical source accessibility. The streaming ingestion pipeline
and scheduled orchestration must be verified separately.
