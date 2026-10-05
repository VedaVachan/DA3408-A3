import argparse
import glob
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, when
from pyspark.sql.types import DoubleType


def average_speed(distance, duration_hours):
    """Average speed in miles/hour."""
    if distance is None or duration_hours is None or duration_hours <= 0:
        return None
    return float(distance / duration_hours)


average_speed_udf = udf(average_speed, DoubleType())


def build_spark():
    return (
        SparkSession.builder
        .appName("DA3408-Spark-Pipeline")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "80")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )


def build_pipeline(spark, input_path, zones_path, mode):
    input_files = sorted(glob.glob(input_path))
    if not input_files:
        raise FileNotFoundError(f"No input files matched: {input_path}")
    trips = spark.read.parquet(*input_files)

    trips = trips.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount"
    )

    trips = trips.dropna(
        subset=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "DOLocationID"
        ]
    )

    trips = trips.dropDuplicates()

    trips = (
        trips
        .withColumn(
            "tpep_pickup_datetime",
            to_timestamp("tpep_pickup_datetime")
        )
        .withColumn(
            "tpep_dropoff_datetime",
            to_timestamp("tpep_dropoff_datetime")
        )
        .withColumn(
            "trip_distance",
            col("trip_distance").cast("double")
        )
    )

    zones = (
        spark.read
        .option("header", True)
        .csv(zones_path)
        .select(
            col("LocationID").cast("long").alias("LocationID"),
            "Borough",
            "Zone",
            "service_zone"
        )
    )

    pickup_zones = zones.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone")
    )

    dropoff_zones = zones.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone")
    )

    trips = trips.join(
        pickup_zones,
        on="PULocationID",
        how="inner"
    )

    trips = trips.join(
        dropoff_zones,
        on="DOLocationID",
        how="inner"
    )

    trips = trips.withColumn(
        "trip_duration_hours",
        (
            col("tpep_dropoff_datetime").cast("long")
            - col("tpep_pickup_datetime").cast("long")
        ) / 3600.0
    )

    if mode == "udf":
        trips = trips.withColumn(
            "average_speed_mph",
            average_speed_udf(
                col("trip_distance"),
                col("trip_duration_hours")
            )
        )

    elif mode == "native":
        trips = trips.withColumn(
            "average_speed_mph",
            when(
                col("trip_duration_hours") > 0,
                col("trip_distance") / col("trip_duration_hours")
            ).otherwise(None)
        )

    else:
        raise ValueError("mode must be 'udf' or 'native'")

    return trips


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument(
        "--mode",
        choices=["udf", "native"],
        default="udf"
    )

    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    start = time.perf_counter()

    trips = build_pipeline(
        spark,
        args.input,
        args.zones,
        args.mode
    )

    trips.write.mode("overwrite").parquet(args.output)

    total_time = time.perf_counter() - start

    result = {
        "framework": "spark",
        "mode": args.mode,
        "total_time_seconds": round(total_time, 3),
        "input": args.input,
        "output": args.output
    }

    print("\n==========================================")
    print("SPARK BENCHMARK COMPLETED")
    print("==========================================")
    print(json.dumps(result))
    print("==========================================\n")

    spark.stop()


if __name__ == "__main__":
    main()
