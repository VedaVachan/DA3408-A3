# DA3408 A3 — Spark vs. Ray: The Data Engineering Duel

## Overview

This project implements the same distributed preprocessing pipeline using Apache Spark and Ray Data and compares their performance on NYC Yellow Taxi Trip Data.

## Pipeline

1. Ingestion of monthly Parquet files
2. Cleansing
   - Null filtering
   - Duplicate removal
   - Timestamp normalization
3. Transformation
   - Pickup and drop-off taxi-zone joins
   - Trip duration calculation
   - Average-speed feature
4. Export to Parquet

Both frameworks were evaluated in:
- Native expression mode
- Python UDF mode

## Benchmark Results

| Framework | Mode | Total Time (s) |
|---|---|---:|
| Spark | Native | 389.903 |
| Spark | UDF | 444.087 |
| Ray | Native | 500.061 |
| Ray | UDF | 556.561 |

## UDF Overhead

- Spark: 54.184 s, approximately 13.9% slower than native.
- Ray: 56.500 s, approximately 11.3% slower than native.

## Parity

All four implementations passed the final parity check.

- Rows: 128,193,192
- Sum distance: approximately 694,895,818.02
- Sum fare: approximately 2,438,620,410.96
- Average duration: approximately 0.286685633 hours
- Valid speed count: 127,617,817
- Average valid speed: approximately 19.6269210472

## Infrastructure

Spark:
- 1 master
- 2 workers
- 10 worker CPU cores total

Ray:
- 1 head
- 2 workers
- 10 CPUs reported by the cluster

## AI-first vs BI-first

The workload is primarily BI-first because it focuses on large-scale structured data ingestion, cleansing, joins, feature computation, and export rather than model training or inference.

AI assistance was used for translating the equivalent Spark and Ray pipeline logic and for performance-tuning considerations. The benchmark methodology and cluster execution were kept consistent across both frameworks.
