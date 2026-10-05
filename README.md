# DA3408 Assignment 3 — Spark vs. Ray

This repository contains the implementation for comparing Apache Spark and
Ray using the NYC Yellow Taxi dataset.

## Project Structure

```text
DA3408-A3/
│
├── spark_clean.py
├── ray_clean.py
├── .gitignore
│
├── data/
│   └── lookup/
│       └── taxi_zone_lookup.csv
│
├── screenshots/
│   ├── spark_master_2_workers.png
│   ├── spark_job_progress.png
│   ├── ray_dashboard_2_nodes.png
│   ├── ray_jobs_history.png
│   └── ray_benchmark_result.png
│
└── report/
    └── DA24B029_A3.pdf
```

## Setup (Once)

```powershell
cd C:\DA3408-A3

python -m venv venv

.\venv\Scripts\Activate.ps1

pip install pyspark==4.2.0 ray pandas pyarrow
```

## Dataset Setup

Place the NYC Yellow Taxi Parquet files in:

```text
data/raw/
```

Place the taxi-zone lookup file in:

```text
data/lookup/taxi_zone_lookup.csv
```

The experiment uses 36 monthly Yellow Taxi Parquet files.

---

# Question 1 — Spark

## Start Spark Master

```powershell
$env:SPARK_HOME="C:\DA3408-A3\spark-4.2.0-bin-hadoop3"
$env:SPARK_MASTER_HOST="10.45.14.171"
$env:SPARK_MASTER_PORT="7077"
$env:SPARK_MASTER_WEBUI_PORT="8080"

& "$env:SPARK_HOME\bin\spark-class.cmd" org.apache.spark.deploy.master.Master
```

Spark Master UI:

```text
http://10.45.14.171:8080
```

## Start Worker 1

Open another PowerShell:

```powershell
& "$env:SPARK_HOME\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker `
  --host 10.45.14.171 `
  --port 7078 `
  --webui-port 8081 `
  --cores 2 `
  --memory 3g `
  spark://10.45.14.171:7077
```

## Start Worker 2

Open another PowerShell:

```powershell
& "$env:SPARK_HOME\bin\spark-class.cmd" org.apache.spark.deploy.worker.Worker `
  --host 10.45.14.171 `
  --port 7079 `
  --webui-port 8082 `
  --cores 2 `
  --memory 3g `
  spark://10.45.14.171:7077
```

## Run Spark

```powershell
cd C:\DA3408-A3
.\venv\Scripts\Activate.ps1

$env:PYSPARK_PYTHON="C:\DA3408-A3\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="C:\DA3408-A3\venv\Scripts\python.exe"
```

```powershell
spark-submit `
  --master spark://10.45.14.171:7077 `
  --deploy-mode client `
  --conf spark.driver.host=10.45.14.171 `
  --conf spark.driver.bindAddress=10.45.14.171 `
  --conf spark.pyspark.python="C:\DA3408-A3\venv\Scripts\python.exe" `
  --conf spark.executorEnv.PYSPARK_PYTHON="C:\DA3408-A3\venv\Scripts\python.exe" `
  --conf spark.executor.cores=2 `
  --conf spark.executor.memory=2500m `
  --conf spark.sql.shuffle.partitions=16 `
  C:\DA3408-A3\spark_clean.py
```

---

# Question 2 — Ray

## Start Ray Head

```powershell
cd C:\DA3408-A3
.\venv\Scripts\Activate.ps1

ray stop

$env:RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER="1"

ray start --head `
  --node-ip-address=10.45.14.171 `
  --port=6379 `
  --dashboard-host=0.0.0.0 `
  --dashboard-port=8265 `
  --num-cpus=2
```

Ray Dashboard:

```text
http://10.45.14.171:8265
```

## Start Ray Worker

Open another PowerShell:

```powershell
cd C:\DA3408-A3
.\venv\Scripts\Activate.ps1

$env:RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER="1"

ray start `
  --address=10.45.14.171:6379 `
  --num-cpus=2
```

## Run Ray

```powershell
cd C:\DA3408-A3
.\venv\Scripts\Activate.ps1

python ray_clean.py
```

---

# Benchmark Result

Ray completed the full pipeline with:

```text
Input files: 36
Input rows: 128,202,548
Total wall time: 348.59 seconds
```

Spark started successfully but the Python workers crashed during execution,
so the complete Spark pipeline did not finish.

---

# Screenshots

The `screenshots/` folder contains:

```text
spark_master_2_workers.png
spark_job_progress.png
ray_dashboard_2_nodes.png
ray_jobs_history.png
ray_benchmark_result.png
```

# Report

The final report is available in:

```text
report/DA24B029_A3.pdf
```
