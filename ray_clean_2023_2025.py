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
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
]

DEDUP_COLUMNS = TRIP_COLUMNS

ZONE_COLUMNS = [
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
]


def prepare_trips(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()

    batch["tpep_pickup_datetime"] = pd.to_datetime(
        batch["tpep_pickup_datetime"], errors="coerce"
    )
    batch["tpep_dropoff_datetime"] = pd.to_datetime(
        batch["tpep_dropoff_datetime"], errors="coerce"
    )
    batch["trip_distance"] = pd.to_numeric(
        batch["trip_distance"], errors="coerce"
    )
    batch["PULocationID"] = pd.to_numeric(
        batch["PULocationID"], errors="coerce"
    ).astype("int64")
    batch["DOLocationID"] = pd.to_numeric(
        batch["DOLocationID"], errors="coerce"
    ).astype("int64")
    batch["fare_amount"] = pd.to_numeric(
        batch["fare_amount"], errors="coerce"
    )

    return batch


def prepare_zones(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()

    batch["LocationID"] = pd.to_numeric(
        batch["LocationID"], errors="coerce"
    ).astype("int64")

    return batch


def drop_duplicates_batch(batch: pd.DataFrame) -> pd.DataFrame:
    return batch.drop_duplicates(subset=DEDUP_COLUMNS)


def add_trip_duration(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()

    duration_seconds = (
        batch["tpep_dropoff_datetime"]
        - batch["tpep_pickup_datetime"]
    ).dt.total_seconds()

    batch["trip_duration_hours"] = duration_seconds / 3600.0

    return batch


def average_speed_udf(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()

    duration = batch["trip_duration_hours"].to_numpy(dtype=np.float64)
    distance = batch["trip_distance"].to_numpy(dtype=np.float64)

    batch["average_speed_mph"] = np.where(
        duration > 0,
        distance / duration,
        np.nan,
    )

    return batch


def process_month(input_file, zones_path, output_dir, mode):
    print()
    print("=" * 60)
    print(f"PROCESSING: {os.path.basename(input_file)}")
    print("=" * 60)

    trips = ray.data.read_parquet(
        input_file,
        columns=TRIP_COLUMNS,
        override_num_blocks=80,
    )


    trips = trips.filter(
        expr=(
            col("tpep_pickup_datetime").is_not_null()
            & col("tpep_dropoff_datetime").is_not_null()
            & col("trip_distance").is_not_null()
            & col("PULocationID").is_not_null()
            & col("DOLocationID").is_not_null()
        )
    )

    print("Removing duplicates...")

    trips = trips.repartition(
        num_blocks=80,
        shuffle=True,
        keys=DEDUP_COLUMNS,
    )

    trips = trips.map_batches(
        drop_duplicates_batch,
        batch_format="pandas",
        batch_size=None,
    )

    trips = trips.map_batches(
        prepare_trips,
        batch_format="pandas",
    )

    trips = trips.map_batches(
        add_trip_duration,
        batch_format="pandas",
    )

    zones = ray.data.read_csv(
        zones_path,
        include_paths=False,
    ).select_columns(ZONE_COLUMNS)

    zones = zones.map_batches(
        prepare_zones,
        batch_format="pandas",
    )

    pickup_zones = zones.rename_columns({
        "LocationID": "PULocationID",
        "Borough": "pickup_borough",
        "Zone": "pickup_zone",
        "service_zone": "pickup_service_zone",
    })

    dropoff_zones = zones.rename_columns({
        "LocationID": "DOLocationID",
        "Borough": "dropoff_borough",
        "Zone": "dropoff_zone",
        "service_zone": "dropoff_service_zone",
    })

    print("Joining pickup zones...")

    trips = trips.join(
        pickup_zones,
        join_type="inner",
        num_partitions=80,
        on=("PULocationID",),
    )

    print("Joining dropoff zones...")

    trips = trips.join(
        dropoff_zones,
        join_type="inner",
        num_partitions=80,
        on=("DOLocationID",),
    )


    if mode == "native":
        print("Calculating average speed using native expression...")

        duration = col("trip_duration_hours")
        sign = duration.sign()
        positive_mask = (sign + sign.abs()) / 2

        trips = trips.with_column(
            "average_speed_mph",
            (col("trip_distance") * positive_mask)
            / (duration * positive_mask),
        )

    elif mode == "udf":
        print("Calculating average speed using Python UDF...")

        trips = trips.map_batches(
            average_speed_udf,
            batch_format="pandas",
        )

    month_name = os.path.splitext(os.path.basename(input_file))[0]
    month_output = os.path.join(output_dir, month_name)

    os.makedirs(month_output, exist_ok=True)

    print(f"Writing {month_output}...")

    trips.write_parquet(
        month_output,
        mode="overwrite",
    )

    del trips
    del zones
    del pickup_zones
    del dropoff_zones

    gc.collect()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--mode",
        choices=["native", "udf"],
        required=True,
    )

    args = parser.parse_args()

    input_files = sorted(glob.glob(args.input))

    if not input_files:
        raise FileNotFoundError(
            f"No input files found: {args.input}"
        )

    print(f"Total monthly files: {len(input_files)}")

    ray.init(
        address="auto",
        ignore_reinit_error=True,
    )

    total_start = time.perf_counter()

    try:
        for i, input_file in enumerate(input_files, start=1):
            print(f"\n[{i}/{len(input_files)}]")

            month_start = time.perf_counter()

            process_month(
                input_file,
                args.zones,
                args.output,
                args.mode,
            )

            month_time = time.perf_counter() - month_start

            print(
                f"Completed {os.path.basename(input_file)} "
                f"in {month_time:.3f} seconds"
            )

    finally:
        total_time = time.perf_counter() - total_start
        ray.shutdown()

    print()
    print("=" * 50)
    print("RAY BENCHMARK COMPLETED")
    print("=" * 50)

    print(json.dumps({
        "framework": "ray",
        "mode": args.mode,
        "total_time_seconds": round(total_time, 3),
        "num_input_files": len(input_files),
        "input": args.input,
        "output": args.output,
        "execution_strategy": "sequential_monthly_processing",
    }))


if __name__ == "__main__":
    main()
