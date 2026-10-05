import os
import time
import pandas as pd
import ray
from ray.data import read_parquet

DATA_DIR = r"C:\DA3408-A3\data\raw"
LOOKUP_FILE = r"C:\DA3408-A3\data\lookup\taxi_zone_lookup.csv"
OUTPUT_DIR = r"C:\DA3408-A3\output\ray"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("\n========== RAY BENCHMARK ==========")
print("Ray version:", ray.__version__)
print("===================================")

start = time.time()

# Connect to the 2-node Ray cluster
ray.init(address="10.78.32.29:6379", ignore_reinit_error=True)

files = [str(x).replace(chr(92), chr(47)) for x in __import__("pathlib").Path(r"C:\DA3408-A3\data\raw").glob("*.parquet")]

print("Number of input files:", len(files))

# 1. INGESTION
from ray.data import from_pandas

columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID"
]

datasets = []

for i, file in enumerate(files, 1):
    df = pd.read_parquet(file, columns=columns)
    print(f"Loaded {i}/{len(files)}: {os.path.basename(file)} -> {len(df):,} rows")
    datasets.append(from_pandas(df))

trips = datasets[0].union(*datasets[1:])

print("Input rows:", trips.count())
# 2. CLEANSING + TRANSFORMATION
lookup = pd.read_csv(LOOKUP_FILE)
lookup["LocationID"] = lookup["LocationID"].astype("int64")

def clean_and_join(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.dropna(
        subset=[
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "DOLocationID"
        ]
    )

    batch = batch.drop_duplicates()

    batch["pickup_datetime"] = pd.to_datetime(
        batch["tpep_pickup_datetime"], errors="coerce"
    )
    batch["dropoff_datetime"] = pd.to_datetime(
        batch["tpep_dropoff_datetime"], errors="coerce"
    )

    batch = batch.dropna(subset=["pickup_datetime", "dropoff_datetime"])

    batch["PULocationID"] = pd.to_numeric(
        batch["PULocationID"], errors="coerce"
    )
    batch["DOLocationID"] = pd.to_numeric(
        batch["DOLocationID"], errors="coerce"
    )

    batch = batch.dropna(subset=["PULocationID", "DOLocationID"])

    batch["PULocationID"] = batch["PULocationID"].astype("int64")
    batch["DOLocationID"] = batch["DOLocationID"].astype("int64")

    # Pickup join
    pickup_lookup = lookup.rename(
        columns={
            "LocationID": "PULocationID",
            "Borough": "pickup_borough",
            "Zone": "pickup_zone"
        }
    )[["PULocationID", "pickup_borough", "pickup_zone"]]

    batch = batch.merge(
        pickup_lookup,
        on="PULocationID",
        how="left"
    )

    # Dropoff join
    dropoff_lookup = lookup.rename(
        columns={
            "LocationID": "DOLocationID",
            "Borough": "dropoff_borough",
            "Zone": "dropoff_zone"
        }
    )[["DOLocationID", "dropoff_borough", "dropoff_zone"]]

    batch = batch.merge(
        dropoff_lookup,
        on="DOLocationID",
        how="left"
    )

    # Custom Python UDF: average speed
    duration_hours = (
        batch["dropoff_datetime"] - batch["pickup_datetime"]
    ).dt.total_seconds() / 3600.0

    batch["average_speed_mph"] = (
        batch["trip_distance"] / duration_hours
    )

    batch.loc[
        (duration_hours <= 0) |
        (batch["average_speed_mph"] <= 0) |
        (batch["average_speed_mph"] > 100),
        "average_speed_mph"
    ] = None

    return batch[
        [
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
        ]
    ]

result = trips.map_batches(
    clean_and_join,
    batch_format="pandas",
    batch_size=50000
)

# 3. EXPORT
result.map_batches(lambda b: (b.to_parquet(os.path.join(OUTPUT_DIR, "part_" + str(__import__("uuid").uuid4()) + ".parquet"), index=False), b)[1], batch_format="pandas").materialize()

total_time = time.time() - start

print("\n========== RAY RESULT ==========")
print("TOTAL WALL TIME:", round(total_time, 2), "seconds")
print("Output directory:", OUTPUT_DIR)
print("================================")

ray.shutdown()


















