from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

PROJECT_ID = "ride-hailing-analytics-512806"
DATASET_ID = "curated_ds"
TABLE_ID = "rides_external"

with DAG(
    dag_id="ride_hailing_daily",
    description="Validate curated ride-hailing data in BigQuery",
    start_date=datetime(2026, 10, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["ride-hailing", "bigquery", "capstone"],
) as dag:

    validate_curated_data = BigQueryCheckOperator(
        task_id="validate_curated_data",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """,
        use_legacy_sql=False,
        location="us-east4",
        gcp_conn_id="google_cloud_default",
    )
    validate_timestamps = BigQueryCheckOperator(
        task_id="validate_timestamps",
        sql=f"""
            SELECT COUNTIF(
                pickup_datetime IS NULL
                OR dropoff_datetime IS NULL
            ) = 0
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """,
        use_legacy_sql=False,
        location="us-east4",
        gcp_conn_id="google_cloud_default",
    )
    validate_trip_distance = BigQueryCheckOperator(
        task_id="validate_trip_distance",
        sql=f"""
            SELECT COUNTIF(
                trip_distance IS NULL
                OR trip_distance <= 0
            ) = 0
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """,
        use_legacy_sql=False,
        location="us-east4",
        gcp_conn_id="google_cloud_default",
    )
    validate_fare_amount = BigQueryCheckOperator(
        task_id="validate_fare_amount",
        sql=f"""
            SELECT COUNTIF(
                fare_amount IS NULL
                OR fare_amount < 0
            ) = 0
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """,
        use_legacy_sql=False,
        location="us-east4",
        gcp_conn_id="google_cloud_default",
    )
validate_curated_data >> validate_timestamps >> validate_trip_distance >> validate_fare_amount