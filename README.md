# DA3408 A3 --- Spark vs. Ray: The Data Engineering Duel

A distributed data engineering benchmark comparing **Apache Spark** and
**Ray Data** on the **NYC Yellow Taxi Trip Dataset**.

The project implements the same preprocessing pipeline in both
frameworks and compares their execution time, resource usage, and the
overhead introduced by Python UDFs.

------------------------------------------------------------------------

## 1. Project Overview

The pipeline processes multiple NYC Yellow Taxi Parquet files through
the following stages:

1.  **Ingestion** --- load monthly Parquet files
2.  **Cleansing**
    -   Remove null/invalid records
    -   Remove duplicates
    -   Normalize timestamps
3.  **Transformation**
    -   Join trip data with the taxi-zone lookup table
    -   Calculate trip duration
    -   Calculate average speed
4.  **Export** --- write the processed data to Parquet

Both Spark and Ray are evaluated in:

-   Native / built-in expression mode
-   Python UDF mode

The same input data and equivalent transformations are used for a fair
comparison.

------------------------------------------------------------------------

## 2. Repository Structure

``` text
DA3408-A3/
│
├── data/
│   ├── raw/                         # NYC Taxi Parquet files (not committed)
│   ├── lookup/
│   │   └── taxi_zone_lookup.csv
│   └── processed/                   # Generated output (not committed)
│
├── docker/
│   ├── spark/
│   │   └── docker-compose.yml
│   └── ray/
│       └── docker-compose.yml
│
├── plots/
│   ├── Spark Peak CPU Usage.png
│   ├── Spark Peak Memory Usage.png
│   ├── Ray Peak CPU Usage.png
│   └── Ray Peak Memory Usage.png
│
├── screenshots/
│   └── ...
│
├── spark_clean.py
├── ray_clean.py
├── spark_clean_2023_2025.py
├── parity_check_pyarrow_2023_2025.py
│
├── .gitignore
├── AI_Disclosure
└── README.md
```

> Large datasets and generated outputs are excluded from GitHub using
> `.gitignore`.

------------------------------------------------------------------------

# 3. Requirements

Recommended environment:

-   Ubuntu / Linux
-   Docker
-   Docker Compose
-   Git
-   Python 3
-   Apache Spark
-   Ray Data

Check the installations:

``` bash
docker --version
docker compose version
git --version
python3 --version
```

------------------------------------------------------------------------

# 4. Clone the Repository

``` bash
git clone https://github.com/VedaVachan/DA3408-A3.git
cd DA3408-A3
```

------------------------------------------------------------------------

# 5. Download the Dataset

The benchmark uses NYC Yellow Taxi trip data.

Create the required directories:

``` bash
mkdir -p data/raw
mkdir -p data/lookup
mkdir -p data/processed
```

Download the taxi-zone lookup table:

``` bash
wget -O data/lookup/taxi_zone_lookup.csv \
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Download the required monthly Yellow Taxi Parquet files into:

``` text
data/raw/
```

For example:

``` bash
wget -P data/raw \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

wget -P data/raw \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet

wget -P data/raw \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

wget -P data/raw \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

Repeat for all required months in the benchmark.

Verify the downloaded files:

``` bash
ls -lh data/raw/
```

------------------------------------------------------------------------

# 6. Spark Setup

Start the Spark cluster:

``` bash
docker compose -f docker/spark/docker-compose.yml up -d
```

Check running containers:

``` bash
docker ps
```

Check the Spark containers:

``` bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

To stop Spark:

``` bash
docker compose -f docker/spark/docker-compose.yml down
```

------------------------------------------------------------------------

# 7. Ray Setup

Start the Ray cluster:

``` bash
docker compose -f docker/ray/docker-compose.yml up -d
```

Check the containers:

``` bash
docker ps
```

To stop Ray:

``` bash
docker compose -f docker/ray/docker-compose.yml down
```

------------------------------------------------------------------------

# 8. Running the Spark Pipeline

Run the Spark implementation:

``` bash
python3 spark_clean.py
```

For the 2023--2025 benchmark:

``` bash
python3 spark_clean_2023_2025.py
```

The Spark pipeline performs:

``` text
Ingestion
    ↓
Cleansing
    ↓
Timestamp normalization
    ↓
Taxi-zone join
    ↓
Trip duration
    ↓
Average speed
    ↓
Parquet export
```

------------------------------------------------------------------------

# 9. Running the Ray Pipeline

Run:

``` bash
python3 ray_clean.py
```

The Ray pipeline performs the same logical operations as the Spark
implementation.

This ensures that the comparison is between the frameworks rather than
between different algorithms.

------------------------------------------------------------------------

# 10. Native vs Python UDF Experiments

The benchmark evaluates two execution modes.

### Native mode

Uses framework-native operations wherever possible.

Examples:

``` text
Spark SQL expressions
Ray Data built-in transformations
```

### Python UDF mode

Uses custom Python functions for selected transformations such as
average-speed calculation.

The purpose is to measure the additional overhead caused by Python-level
execution.

------------------------------------------------------------------------

# 11. Parity Check

The Spark and Ray outputs should represent the same logical
transformation.

Run:

``` bash
python3 parity_check_pyarrow_2023_2025.py
```

The parity check verifies that the generated datasets are equivalent
with respect to the required output columns and values.

------------------------------------------------------------------------

# 12. Monitoring Resource Usage

Docker statistics can be used to monitor CPU and memory usage.

For Spark:

``` bash
docker stats --no-stream \
  --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" \
  da3408-spark-master \
  da3408-spark-worker-1 \
  da3408-spark-worker-2
```

For Ray, use the corresponding Ray container names:

``` bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Then:

``` bash
docker stats --no-stream
```

The collected measurements are summarized in the plots under `plots/`.

------------------------------------------------------------------------

# 13. Benchmark Results

  Framework           Mode   Total Time (s)
  ----------- ------------ ----------------
  Spark             Native          389.903
  Spark         Python UDF          444.087
  Ray               Native          500.061
  Ray           Python UDF          556.561

### UDF Overhead

  Framework     Native (s)   UDF (s)   Overhead
  ----------- ------------ --------- ----------
  Spark            389.903   444.087    \~13.9%
  Ray              500.061   556.561    \~11.3%

The benchmark indicates that Spark completed the tested workload faster
than Ray for both native and Python-UDF execution modes.

------------------------------------------------------------------------

# 14. Resource Usage Plots

The repository contains CPU and memory usage plots:

``` text
plots/
├── Spark Peak CPU Usage.png
├── Spark Peak Memory Usage.png
├── Ray Peak CPU Usage.png
└── Ray Peak Memory Usage.png
```

These plots are used alongside execution time to compare the resource
characteristics of both frameworks.

------------------------------------------------------------------------

# 15. Reproducibility

For a fair comparison:

-   Use the same input dataset for Spark and Ray.
-   Use equivalent transformation logic.
-   Run both frameworks under comparable hardware/resource limits.
-   Repeat benchmark runs when necessary.
-   Record execution time separately for native and UDF modes.
-   Record peak CPU and memory usage.
-   Verify output parity before comparing performance.

------------------------------------------------------------------------

# 16. Complete Workflow

A typical end-to-end run is:

``` bash
# 1. Clone repository
git clone https://github.com/VedaVachan/DA3408-A3.git
cd DA3408-A3

# 2. Create data directories
mkdir -p data/raw data/lookup data/processed

# 3. Download lookup table
wget -O data/lookup/taxi_zone_lookup.csv \
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# 4. Download the required NYC Taxi Parquet files
#    Place them inside data/raw/

# 5. Start Spark
docker compose -f docker/spark/docker-compose.yml up -d

# 6. Run Spark benchmark
python3 spark_clean_2023_2025.py

# 7. Stop Spark
docker compose -f docker/spark/docker-compose.yml down

# 8. Start Ray
docker compose -f docker/ray/docker-compose.yml up -d

# 9. Run Ray benchmark
python3 ray_clean.py

# 10. Stop Ray
docker compose -f docker/ray/docker-compose.yml down

# 11. Verify output parity
python3 parity_check_pyarrow_2023_2025.py
```

------------------------------------------------------------------------

# 17. GitHub Notes

Large datasets are intentionally **not committed** to the repository.

Before committing changes:

``` bash
git status
```

Add files:

``` bash
git add .
```

Commit:

``` bash
git commit -m "Update Spark vs Ray benchmark"
```

Push:

``` bash
git push origin main
```

------------------------------------------------------------------------

# 18. Conclusion

This project provides an apples-to-apples comparison of Apache Spark and
Ray Data for a distributed NYC Taxi preprocessing workload.

The benchmark compares:

-   Execution time
-   Native vs Python UDF performance
-   UDF overhead
-   CPU utilization
-   Memory utilization
-   Output parity

The results can be used to understand the practical trade-offs between
Spark and Ray for distributed data preprocessing workloads.

------------------------------------------------------------------------

## Author

**DA3408 --- Data Engineering Assignment 3**

Spark vs. Ray Benchmark
