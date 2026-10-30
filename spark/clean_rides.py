
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_PATH = (
    "gs://ride-hailing-analytics-512806-raw/"
    "yellow_tripdata_2022-03.parquet"
)
OUTPUT_PATH = (
    "gs://ride-hailing-analytics-512806-curated/"
    "rides/"
)


def main():
    spark = (
        SparkSession.builder
        .appName("RideHailingCleanRides")
        .getOrCreate()
    )

    # Read historical Parquet data.
    df = spark.read.parquet(INPUT_PATH)

    # Standardize source column names and types.
    df = (
        df.withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("RatecodeID", "rate_code")
        .withColumn("vendor_id", F.col("vendor_id").cast("string"))
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn(
            "pickup_location_id",
            F.col("pickup_location_id").cast("string"),
        )
        .withColumn(
            "dropoff_location_id",
            F.col("dropoff_location_id").cast("string"),
        )
    )

    # Keep only the March 2022 historical reporting period.
    df = df.filter(
        (F.to_date("pickup_datetime") >= F.lit("2022-03-01"))
        & (F.to_date("pickup_datetime") <= F.lit("2022-03-31"))
    )

    # Remove invalid records.
    df = df.filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        & F.col("trip_distance").isNotNull()
        & (F.col("trip_distance") > 0)
        & F.col("fare_amount").isNotNull()
        & (F.col("fare_amount") >= 0)
        & F.col("total_amount").isNotNull()
        & (F.col("total_amount") >= 0)
    )

    # Remove exact duplicate records.
    df = df.dropDuplicates()

    # Derive trip duration and revenue metrics.
    df = (
        df.withColumn(
            "trip_duration_minutes",
            (
                F.col("dropoff_datetime").cast("long")
                - F.col("pickup_datetime").cast("long")
            ) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn(
            "average_speed_mph",
            F.col("trip_distance")
            / (F.col("trip_duration_minutes") / 60.0),
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                F.col("tip_amount") / F.col("fare_amount") * 100.0,
            ),
        )
        .withColumn("processing_ts", F.current_timestamp())
    )

    # Exclude implausibly long trips and invalid derived values.
    df = df.filter(
        (F.col("trip_duration_minutes") > 0)
        & (F.col("trip_duration_minutes") <= 240)
    )

    # Basic data-quality metrics in the application logs.
    print("Input rows:", spark.read.parquet(INPUT_PATH).count())
    print("Cleaned rows:", df.count())

    # Write curated Parquet, partitioned by pickup date.
    (
        df.write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(OUTPUT_PATH)
    )

    print("Curated output:", OUTPUT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
