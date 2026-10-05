import math
from pathlib import Path
import pyarrow.dataset as ds

PATHS = {
    "spark_native": Path("output/spark_native_full"),
    "spark_udf": Path("output/spark_udf_full"),
    "ray_native": Path("output/ray_native_full"),
    "ray_udf": Path("output/ray_udf_full"),
}

def summarize(path):
    dataset = ds.dataset(str(path), format="parquet")
    table = dataset.to_table()

    def col(name):
        return table[name].to_pylist()

    distance = col("trip_distance")
    fare = col("fare_amount")
    duration = col("trip_duration_hours")
    speed = col("average_speed")

    valid_speed = [x for x in speed if x is not None and math.isfinite(float(x))]
    durations = [x for x in duration if x is not None]

    return {
        "rows": table.num_rows,
        "sum_distance": sum(float(x) for x in distance if x is not None),
        "sum_fare": sum(float(x) for x in fare if x is not None),
        "avg_duration": sum(float(x) for x in durations) / len(durations),
        "valid_speed_count": len(valid_speed),
        "avg_valid_speed": sum(float(x) for x in valid_speed) / len(valid_speed),
        "min_valid_speed": min(float(x) for x in valid_speed),
        "max_valid_speed": max(float(x) for x in valid_speed),
    }

def compare(a, b, sa, sb):
    keys = sa.keys()
    ok = True
    for k in keys:
        if k == "rows" or k == "valid_speed_count":
            same = sa[k] == sb[k]
        else:
            same = math.isclose(sa[k], sb[k], rel_tol=1e-9, abs_tol=1e-6)
        print(f"  {k:20s}: {'PASS' if same else 'FAIL'}")
        ok &= same
    return ok

summaries = {}
for name, path in PATHS.items():
    print(f"\nReading {name}: {path}")
    summaries[name] = summarize(path)
    print(summaries[name])

pairs = [
    ("spark_native", "spark_udf"),
    ("ray_native", "ray_udf"),
    ("spark_native", "ray_native"),
    ("spark_udf", "ray_udf"),
]

all_ok = True
for a, b in pairs:
    print(f"\n{a} vs {b}")
    all_ok &= compare(a, b, summaries[a], summaries[b])

print("\n" + "=" * 60)
print(f"FINAL PARITY: {'PASS' if all_ok else 'FAIL'}")
print("=" * 60)
