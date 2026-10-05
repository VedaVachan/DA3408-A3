import argparse
import gc
import glob
import json
import os
import time
import numpy as np
import pandas as pd
import ray
from ray.data.expressions import col

TRIP_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "PULocationID", "DOLocationID", "fare_amount"
]

ZONE_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]

DEDUP_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "PULocationID", "DOLocationID", "fare_amount"
]

def prepare_trips(df):
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], errors="coerce"
    )
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
    df["PULocationID"] = pd.to_numeric(
        df["PULocationID"], errors="coerce"
    ).astype("Int64")
    df["DOLocationID"] = pd.to_numeric(
        df["DOLocationID"], errors="coerce"
    ).astype("Int64")
    return df

def dedup_batch(df):
    return df.drop_duplicates(subset=DEDUP_COLUMNS)

def add_duration(df):
    df["trip_duration_hours"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 3600.0
    )
    return df

def average_speed_udf(df):
    duration = df["trip_duration_hours"].to_numpy()
    distance = df["trip_distance"].to_numpy()
    df["average_speed"] = np.where(
        duration > 0,
        distance / duration,
        np.nan
    )
    return df

def process_month(path, zones_path, output_path, mode):
    ds = ray.data.read_parquet(path, override_num_blocks=80)
    ds = ds.select_columns(TRIP_COLUMNS)
    ds = ds.filter(lambda r: all(r.get(c) is not None for c in TRIP_COLUMNS))
    ds = ds.repartition(80)
    ds = ds.map_batches(dedup_batch, batch_format="pandas")
    ds = ds.map_batches(prepare_trips, batch_format="pandas")
    ds = ds.map_batches(add_duration, batch_format="pandas")

    zones = pd.read_csv(zones_path)
    pickup = zones[ZONE_COLUMNS].rename(columns={
        "LocationID": "PULocationID",
        "Borough": "PU_Borough",
        "Zone": "PU_Zone",
        "service_zone": "PU_service_zone"
    })
    dropoff = zones[ZONE_COLUMNS].rename(columns={
        "LocationID": "DOLocationID",
        "Borough": "DO_Borough",
        "Zone": "DO_Zone",
        "service_zone": "DO_service_zone"
    })

    ds = ds.join(pickup, on="PULocationID", how="inner")
    ds = ds.join(dropoff, on="DOLocationID", how="inner")
    ds = ds.repartition(80)

    if mode == "udf":
        ds = ds.map_batches(average_speed_udf, batch_format="pandas")
    else:
        ds = ds.map(
            lambda r: {
                **r,
                "average_speed": (
                    r["trip_distance"] / r["trip_duration_hours"]
                    if r["trip_duration_hours"] is not None
                    and r["trip_duration_hours"] > 0
                    else None
                )
            }
        )

    ds.write_parquet(output_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=["native", "udf"], required=True)
    args = parser.parse_args()

    ray.init(address="auto")

    files = sorted(glob.glob(args.input))
    start = time.perf_counter()

    for path in files:
        month = os.path.basename(path).replace("yellow_tripdata_", "").replace(".parquet", "")
        print(f"Running Ray {args.mode}: {month}")
        process_month(
            path,
            args.zones,
            os.path.join(args.output, month),
            args.mode
        )
        gc.collect()

    elapsed = time.perf_counter() - start
    print("RAY BENCHMARK COMPLETED")
    print(json.dumps({
        "framework": "ray",
        "mode": args.mode,
        "total_time_seconds": elapsed,
        "num_input_files": len(files),
        "input": args.input,
        "output": args.output,
        "execution_strategy": "sequential_monthly_processing"
    }))

if __name__ == "__main__":
    main()
