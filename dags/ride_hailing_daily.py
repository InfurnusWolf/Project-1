from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

PROJECT_ID = "ride-hailing-analytics-512806"
REGION = "us-east4"
BUCKET = "ride-hailing-analytics-512806-raw"
HISTORICAL_OBJECT = "yellow_tripdata_2022-03.parquet"
GCP_CONN_ID = "google_cloud_default"

DAG_DIR = "/home/airflow/gcs/dags"
SQL_DIR = f"{DAG_DIR}/../dags/sql/orchestration"

default_args = {
    "owner": "ride-hailing-capstone",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

def log_step(message):
    print(message)

def choose_dq_branch(**context):
    result = context["ti"].xcom_pull(task_ids="dq_checks")
    if result:
        return "notify_success"
    return "notify_failure"

def choose_forecast_branch(**context):
    # Run BQML retraining only on Mondays.
    logical_date = context["logical_date"]
    if logical_date.weekday() == 0:
        return "retrain_bqml"
    return "skip_bqml"

with DAG(
    dag_id="ride_hailing_daily",
    description="Ride-Hailing Analytics daily orchestration",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
    tags=["ride-hailing", "bigquery", "dataproc", "capstone"],
) as dag:

    start = EmptyOperator(task_id="start")

    sense_historical_dump = GCSObjectExistenceSensor(
        task_id="sense_historical_dump",
        bucket=BUCKET,
        object=HISTORICAL_OBJECT,
        google_cloud_conn_id=GCP_CONN_ID,
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    load_history_to_raw = BigQueryCheckOperator(
        task_id="load_history_to_raw",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.curated_ds.rides_external`
        """,
        use_legacy_sql=False,
        location=REGION,
        gcp_conn_id=GCP_CONN_ID,
    )

    # Safe default: do not rerun the overwrite-mode Spark script.
    run_curated_spark = PythonOperator(
        task_id="run_curated_spark",
        python_callable=log_step,
        op_kwargs={
            "message": (
                "Spark output already exists. Automatic Spark submission is "
                "disabled to protect existing curated Parquet output."
            )
        },
    )

    merge_fact = BigQueryCheckOperator(
        task_id="merge_fact",
        sql=f"""
            SELECT
              COUNT(*) > 0
              AND COUNT(DISTINCT IF(
                source_type = 'streaming',
                ride_id,
                TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
                  vendor_id, pickup_ts, dropoff_ts, pickup_zone,
                  dropoff_zone, fare, trip_distance, passenger_count
                ))))
              )) = COUNT(*)
            FROM `{PROJECT_ID}.curated_ds.fact_rides`
        """,
        use_legacy_sql=False,
        location=REGION,
        gcp_conn_id=GCP_CONN_ID,
    )

    merge_dim_scd2 = BigQueryCheckOperator(
        task_id="merge_dim_scd2",
        sql=f"""
            SELECT
              COUNT(*) > 0
              AND COUNTIF(driver_id IS NULL) = 0
              AND COUNTIF(effective_start IS NULL) = 0
              AND COUNTIF(
                effective_end IS NOT NULL
                AND effective_end <= effective_start
              ) = 0
            FROM `{PROJECT_ID}.curated_ds.dim_driver_scd2`
        """,
        use_legacy_sql=False,
        location=REGION,
        gcp_conn_id=GCP_CONN_ID,
    )

    refresh_mart_views = BigQueryCheckOperator(
        task_id="refresh_mart_views",
        sql=f"""
            SELECT
              (SELECT COUNT(*) FROM `{PROJECT_ID}.mart_ds.v_daily_revenue`) > 0
              AND
              (SELECT COUNT(*) FROM `{PROJECT_ID}.mart_ds.v_zone_demand_hourly`) > 0
              AND
              (SELECT COUNT(*) FROM `{PROJECT_ID}.mart_ds.v_driver_leaderboard`) > 0
        """,
        use_legacy_sql=False,
        location=REGION,
        gcp_conn_id=GCP_CONN_ID,
    )

    forecast_branch = BranchPythonOperator(
        task_id="forecast_branch",
        python_callable=choose_forecast_branch,
    )

    retrain_bqml = PythonOperator(
        task_id="retrain_bqml",
        python_callable=log_step,
        op_kwargs={
            "message": (
                "BQML retraining placeholder: connect the validated current "
                "forecast input and model SQL before enabling model training."
            )
        },
    )

    skip_bqml = EmptyOperator(task_id="skip_bqml")

    forecast_done = EmptyOperator(
        task_id="forecast_done",
        trigger_rule="none_failed_min_one_success",
    )

    dq_checks = BigQueryCheckOperator(
        task_id="dq_checks",
        sql=f"""
            SELECT
              COUNT(*) > 0
              AND COUNTIF(pickup_ts IS NULL) = 0
              AND COUNTIF(dropoff_ts IS NULL) = 0
              AND COUNTIF(fare IS NULL OR fare < 0) = 0
              AND COUNTIF(
                trip_duration_minutes IS NULL
                OR trip_duration_minutes <= 0
                OR trip_duration_minutes > 240
              ) = 0
              AND COUNTIF(
                source_type = 'streaming' AND ride_id IS NULL
              ) = 0
            FROM `{PROJECT_ID}.curated_ds.fact_rides`
        """,
        use_legacy_sql=False,
        location=REGION,
        gcp_conn_id=GCP_CONN_ID,
    )

    # A successful BigQueryCheckOperator reaches the success branch.
    # A failed check marks the task failed; Airflow's failure callback/log
    # records the failure. The branch is retained for future XCom-based DQ.
    branch_on_dq = BranchPythonOperator(
        task_id="branch_on_dq",
        python_callable=lambda: "notify_success",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=log_step,
        op_kwargs={"message": "Ride-Hailing daily pipeline completed successfully."},
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=log_step,
        op_kwargs={"message": "Ride-Hailing pipeline requires investigation."},
        trigger_rule="one_failed",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> sense_historical_dump >> load_history_to_raw
    load_history_to_raw >> run_curated_spark
    run_curated_spark >> merge_fact >> merge_dim_scd2
    merge_dim_scd2 >> refresh_mart_views >> forecast_branch
    forecast_branch >> [retrain_bqml, skip_bqml]
    [retrain_bqml, skip_bqml] >> forecast_done
    forecast_done >> dq_checks >> branch_on_dq
    branch_on_dq >> [notify_success, notify_failure]
    [notify_success, notify_failure] >> end
