from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, broadcast, udf
)
from pyspark.sql.types import DoubleType
import time
import os

BASE = r"C:\DA3408-A3"
RAW = os.path.join(BASE, "data", "raw")
LOOKUP = os.path.join(BASE, "data", "lookup", "taxi_zone_lookup.csv")
OUTPUT = os.path.join(BASE, "output", "spark")

os.makedirs(OUTPUT, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Spark_Cleaning")
    .config("spark.sql.shuffle.partitions", "16").config("spark.python.worker.faulthandler.enabled", "true").config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
    .config("spark.sql.execution.pythonUDF.arrow.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========== SPARK BENCHMARK ==========")
print("Spark version:", spark.version)
print("======================================\n")

total_start = time.perf_counter()

# ---------------------------------------------------------
# 1. INGESTION
# ---------------------------------------------------------
ingest_start = time.perf_counter()

files = [
    os.path.join(RAW, f)
    for f in os.listdir(RAW)
    if f.endswith(".parquet")
]

files.sort()

print("Number of input files:", len(files))

trips = spark.read.parquet(*files)

print("Input columns:", len(trips.columns))
print("Input partitions:", trips.rdd.getNumPartitions())

ingest_time = time.perf_counter() - ingest_start
print(f"\nIngestion read setup time: {ingest_time:.2f} seconds")

# ---------------------------------------------------------
# 2. CLEANING
# ---------------------------------------------------------
clean_start = time.perf_counter()

required_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID"
]

trips = trips.select(*required_columns)

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
        "pickup_datetime",
        to_timestamp("tpep_pickup_datetime")
    )
    .withColumn(
        "dropoff_datetime",
        to_timestamp("tpep_dropoff_datetime")
    )
)

clean_time = time.perf_counter() - clean_start
print(f"Cleaning transformation setup time: {clean_time:.2f} seconds")

# ---------------------------------------------------------
# 3. LOOKUP TABLE
# ---------------------------------------------------------
lookup = spark.read.option("header", "true").csv(LOOKUP)

pickup_lookup = (
    lookup
    .select(
        col("LocationID").cast("int").alias("PU_LocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone")
    )
)

dropoff_lookup = (
    lookup
    .select(
        col("LocationID").cast("int").alias("DO_LocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone")
    )
)

# ---------------------------------------------------------
# 4. BROADCAST JOINS
# ---------------------------------------------------------
join_start = time.perf_counter()

trips = (
    trips
    .join(
        broadcast(pickup_lookup),
        col("PULocationID") == col("PU_LocationID"),
        "left"
    )
    .drop("PU_LocationID")
    .join(
        broadcast(dropoff_lookup),
        col("DOLocationID") == col("DO_LocationID"),
        "left"
    )
    .drop("DO_LocationID")
)

# ---------------------------------------------------------
# 5. PYTHON UDF - AVERAGE SPEED
# ---------------------------------------------------------
def calculate_average_speed(distance, pickup, dropoff):
    if distance is None or pickup is None or dropoff is None:
        return None

    seconds = (dropoff - pickup).total_seconds()

    if seconds <= 0:
        return None

    return float(distance / (seconds / 3600.0))


average_speed_udf = udf(
    calculate_average_speed,
    DoubleType()
)

trips = trips.withColumn(
    "average_speed_mph",
    average_speed_udf(
        col("trip_distance"),
        col("pickup_datetime"),
        col("dropoff_datetime")
    )
)

trips = trips.filter(
    col("average_speed_mph").isNotNull()
)

join_time = time.perf_counter() - join_start
print(f"Join + UDF transformation setup time: {join_time:.2f} seconds")

# ---------------------------------------------------------
# 6. FINAL OUTPUT COLUMNS
# ---------------------------------------------------------
result = trips.select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "average_speed_mph"
)

# ---------------------------------------------------------
# 7. EXPORT
# ---------------------------------------------------------
export_start = time.perf_counter()

(
    result
    .repartition(16)
    .write
    .mode("overwrite")
    .parquet(OUTPUT)
)

export_time = time.perf_counter() - export_start

total_time = time.perf_counter() - total_start

print("\n========== BENCHMARK RESULT ==========")
print(f"Input files       : {len(files)}")
print(f"Cleaning setup    : {clean_time:.2f} sec")
print(f"Join + UDF setup  : {join_time:.2f} sec")
print(f"Export time       : {export_time:.2f} sec")
print(f"TOTAL WALL TIME   : {total_time:.2f} sec")
print(f"TOTAL WALL TIME   : {total_time/60:.2f} min")
print("Output:", OUTPUT)
print("======================================\n")

spark.stop()


