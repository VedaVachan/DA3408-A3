import math
import pyarrow.dataset as ds
import pyarrow.compute as pc


PATHS = {
    "spark_native": "output/spark_native_full",
    "spark_udf": "output/spark_udf_full",
    "ray_native": "output/ray_native_full",
    "ray_udf": "output/ray_udf_full",
}

COLUMNS = [
    "trip_distance",
    "fare_amount",
    "trip_duration_hours",
    "average_speed_mph",
]


def stats(path):
    dataset = ds.dataset(path, format="parquet")

    rows = 0
    distance_sum = 0.0
    fare_sum = 0.0
    duration_sum = 0.0
    duration_count = 0

    speed_sum = 0.0
    speed_count = 0
    speed_min = math.inf
    speed_max = -math.inf

    scanner = dataset.scanner(columns=COLUMNS, batch_size=65536)

    for batch in scanner.to_batches():
        rows += batch.num_rows

        distance = batch.column("trip_distance")
        fare = batch.column("fare_amount")
        duration = batch.column("trip_duration_hours")
        speed = batch.column("average_speed_mph")

        distance_sum += pc.sum(distance).as_py() or 0.0
        fare_sum += pc.sum(fare).as_py() or 0.0

        valid_duration = pc.drop_null(duration)
        if len(valid_duration) > 0:
            duration_sum += pc.sum(valid_duration).as_py()
            duration_count += len(valid_duration)

        valid_speed = pc.drop_null(speed)
        if len(valid_speed) > 0:
            finite = pc.filter(
                valid_speed,
                pc.is_finite(valid_speed)
            )

            if len(finite) > 0:
                speed_sum += pc.sum(finite).as_py()
                speed_count += len(finite)

                bmin = pc.min(finite).as_py()
                bmax = pc.max(finite).as_py()

                speed_min = min(speed_min, bmin)
                speed_max = max(speed_max, bmax)

    return {
        "rows": rows,
        "sum_distance": distance_sum,
        "sum_fare": fare_sum,
        "avg_duration": (
            duration_sum / duration_count
            if duration_count else None
        ),
        "valid_speed_count": speed_count,
        "avg_valid_speed": (
            speed_sum / speed_count
            if speed_count else None
        ),
        "min_valid_speed": (
            speed_min if speed_count else None
        ),
        "max_valid_speed": (
            speed_max if speed_count else None
        ),
    }


results = {}

for name, path in PATHS.items():
    print(f"\nReading {name}: {path}")
    results[name] = stats(path)
    print(results[name])


def close(a, b, tol=1e-8):
    if a is None or b is None:
        return a == b
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def compare(a_name, b_name):
    a = results[a_name]
    b = results[b_name]

    checks = {
        "rows": a["rows"] == b["rows"],
        "sum_distance": close(a["sum_distance"], b["sum_distance"]),
        "sum_fare": close(a["sum_fare"], b["sum_fare"]),
        "avg_duration": close(a["avg_duration"], b["avg_duration"]),
        "valid_speed_count": (
            a["valid_speed_count"] == b["valid_speed_count"]
        ),
        "avg_valid_speed": close(
            a["avg_valid_speed"], b["avg_valid_speed"]
        ),
        "min_valid_speed": close(
            a["min_valid_speed"], b["min_valid_speed"]
        ),
        "max_valid_speed": close(
            a["max_valid_speed"], b["max_valid_speed"]
        ),
    }

    print(f"\n{a_name} vs {b_name}")
    for key, value in checks.items():
        print(f"  {key:20s}: {'PASS' if value else 'FAIL'}")

    return all(checks.values())


ok = True

ok &= compare("spark_native", "spark_udf")
ok &= compare("ray_native", "ray_udf")
ok &= compare("spark_native", "ray_native")
ok &= compare("spark_udf", "ray_udf")

print("\n" + "=" * 60)
print("FINAL PARITY:", "PASS" if ok else "FAIL")
print("=" * 60)
