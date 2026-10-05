import argparse
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, when
from pyspark.sql.types import DoubleType

def average_speed(distance, duration_hours):
    if distance is None or duration_hours is None or duration_hours <= 0:
        return None
    return float(distance / duration_hours)

def build_spark():
    return (
        SparkSession.builder
        .appName("DA3408-Spark-Pipeline")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "80")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )

def run_pipeline(spark, input_path, zones_path, output_path, mode):
    trips = (
        spark.read.parquet(input_path)
        .select(
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance", "PULocationID", "DOLocationID", "fare_amount"
        )
        .dropna()
        .dropDuplicates()
        .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        .withColumn("trip_distance", col("trip_distance").cast("double"))
    )

    zones = spark.read.option("header", True).csv(zones_path)

    pickup = zones.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("PU_Borough"),
        col("Zone").alias("PU_Zone"),
        col("service_zone").alias("PU_service_zone")
    )
    dropoff = zones.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("DO_Borough"),
        col("Zone").alias("DO_Zone"),
        col("service_zone").alias("DO_service_zone")
    )

    result = trips.join(pickup, "PULocationID", "inner")
    result = result.join(dropoff, "DOLocationID", "inner")

    result = result.withColumn(
        "trip_duration_hours",
        (col("tpep_dropoff_datetime").cast("long") -
         col("tpep_pickup_datetime").cast("long")) / 3600.0
    )

    if mode == "udf":
        speed_udf = udf(average_speed, DoubleType())
        result = result.withColumn(
            "average_speed",
            speed_udf(col("trip_distance"), col("trip_duration_hours"))
        )
    else:
        result = result.withColumn(
            "average_speed",
            when(
                col("trip_duration_hours") > 0,
                col("trip_distance") / col("trip_duration_hours")
            )
        )

    result.write.mode("overwrite").parquet(output_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=["native", "udf"], required=True)
    args = parser.parse_args()

    start = time.perf_counter()
    spark = build_spark()
    try:
        run_pipeline(spark, args.input, args.zones, args.output, args.mode)
    finally:
        spark.stop()

    elapsed = time.perf_counter() - start
    print("SPARK BENCHMARK COMPLETED")
    print(json.dumps({
        "framework": "spark",
        "mode": args.mode,
        "total_time_seconds": elapsed,
        "input": args.input,
        "output": args.output
    }))

if __name__ == "__main__":
    main()
