**Danish Weather & Energy Data Pipeline with Machine Learning**

A complete real-time system that fetches weather and energy data, trains ML models, and generates hourly energy consumption predictions for Denmark's DK1 and DK2 price areas 1 hour ahead.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────────┤
│  DMI Weather API          │      Energinet API                      │
│  (141 stations)           │      (Production & Consumption)         │
└────────┬──────────────────┴──────────────┬───────────────────────────┘
         │                                 │
         v                                 v
┌─────────────────────┐          ┌──────────────────────────┐
│ Weather Fetcher     │          │ Energy Actual Fetcher    │
│ (5 mins, 141 stn)   │          │ (hourly)                 │
│ (Avro Files)        │          │ (Avro Files)             │  
└────────┬────────────┘          └────────┬─────────────────┘
         │                                │
         v                                v
┌─────────────────────────────────────────────────────────────────────┐
│                          KAFKA TOPICS                                │
├─────────────────────────────────────────────────────────────────────┤
│  weather_hourly_ml        │      energy_actual                      │
└────────┬──────────────────┴──────────────┬───────────────────────────┘
         │                                 │
         v                                 v
┌────────────────┐                  ┌─────────────┐
│ Kafka Connect  │                  │ Kafka       │
│ HDFS Sinks     │                  │ Connect     │
└────────┬───────┘                  └──────┬──────┘
         v                                 v
┌─────────────────┐               ┌─────────────────┐
│ HDFS            │               │ HDFS            │
│ (Parquet files) │               │ (Parquet files) │
└────────┬────────┘               └─────────────────┘
         v                                 |
┌────────────────┐                         |
│ Hive Tables    │                         |
│ (external)     │                         |
└────────┬───────┘                         |
         v                                 |
┌─────────────────────┐                    |
│ Prediction Service  │ <─ ─ ─ ─ ─ ─ ─ ─ ─ ─ 
│ (SparkML, Hive)     │
└────────┬────────────┘
         │
         v
┌─────────────────────────────────────────────────────────────────────┐
│                    KAFKA TOPICS (CONSUMPTION)                       │
├─────────────────────────────────────────────────────────────────────┤
│  energy_predictions (from ML)      energy_actual (from Fetcher)     │
└────────┬───────────────────────────────────┬────────────────────────┘
         │                                   │
         │        ┌──────────────────────────┘
         │        │
         v        v
┌─────────────────────────┐         ┌────────────────┐
│    Database Writer      │         │    BACKEND     │
│       (Node.js)         │         │   (Node.js)    │
└────────────┬────────────┘         └────┬───────▲───┘
             │                           │       │
             v                           │       │
┌─────────────────────────┐              │       │
│      TimescaleDB        │ <────────────┘       │
│     (PostgreSQL)        │                      │
└─────────────────────────┘                      │
                                                 │
                                                 v
                                        ┌────────────────┐
                                        │    FRONTEND    │
                                        │    (React)     │
                                        └────────────────┘
```

---

## Quick Start

### Prerequisites
- Kubernetes cluster
- HDFS (NameNode + DataNodes)
- Hive Metastore
- Kafka cluster
- Kafka Connect
- SparkML
- Spark
- React
- TimescaleDB
- Node.js

### Step 1: Deploy Infrastructure

# BigData Energy Platform Deployment Guide

This guide covers how to deploy the Energy Platform using Helm, including how to manage the heavy ML training job.

## Prerequisites

*   Kubernetes Cluster
*   Helm 3+
*   Docker (if building images)
*   `kubectl` configured for your cluster

## 1. Standard Deployment (No Training)

This is the default mode. It deploys all services (Kafka, Hive, HDFS, Backend, Frontend, Fetchers) but **skips** the heavy ML model training job. Use this for routine updates or restarting services.

```bash
# Deploy or Upgrade
helm upgrade --install energy-platform ./energy-platform -n <your-namespace>
```

*   **What happens:**
    *   Updates deployments (Backend, Frontend, etc.).
    *   Updates configuration.
    *   **Does NOT** re-train the ML model.
    *   **Deletes** the `system-initialization` job if it exists (cleaning up resources).

## 2. Deployment with ML Training (Manual Trigger)

Use this mode when you want to **re-train the ML model** based on the latest data in HDFS. This runs the `system-initialization` job, which aggregates historical data and trains the Random Forest model.

```bash
# Deploy and Trigger Training
helm upgrade --install energy-platform ./energy-platform -n <your-namespace> --set jobs.initializeSystem.enabled=true
```

*   **What happens:**
    *   Deploys/Updates all standard services.
    *   **Creates and Runs** the `system-initialization` job.
    *   The job will:
        1.  Aggregate historical weather/energy data.
        2.  Train a new ML model.
        3.  Save the model to `/data/SparkML/models` and HDFS.

### Monitoring the Training

Since the training job can take 10-30 minutes, you should monitor it:

```bash
# Check status
kubectl get job system-initialization -n <your-namespace>

# Follow logs
kubectl logs -f job/system-initialization -n <your-namespace>
```

### After Training

Once the job shows `Complete` or the logs say `INITIALIZATION COMPLETE`:
1.  The new model is saved.
2.  The `aggregate-and-predict-service` will pick it up on its next restart (or you can roll it out manually).

To clean up the job pod (and save resources), simply run the **Standard Deployment** command again (without the flag).

## 3. Full System Reset (Data Purge)

If you need to completely reset the data (e.g., schema changes):

1.  **Delete Jobs:**
    ```bash
    kubectl delete jobs --all -n <your-namespace>
    ```

2.  **Purge HDFS Data:**
    ```bash
    kubectl exec -it namenode-0 -n <your-namespace> -- hdfs dfs -rm -r /user/hive/warehouse/*
    ```

3.  **Re-deploy (with Training):**
    ```bash
    helm upgrade --install energy-platform ./energy-platform -n <your-namespace> --set jobs.initializeSystem.enabled=true
    ```

## 4. Accessing the Application

To access the frontend and backend services from your local machine, you need to set up port forwarding:

```bash
# Forward Frontend (React) to localhost:3000
kubectl port-forward svc/frontend-service 3000:80 -n <your-namespace>

# Forward Backend (Node.js) to localhost:8080
kubectl port-forward svc/backend-service 8080:8080 -n <your-namespace>
```

Now you can access:
- **Frontend:** http://localhost:3000
- **Backend API:** http://localhost:8080

## 5. Maintenance & Optimization

### Compacting Small Files
If the system accumulates too many small files in HDFS (e.g., after a bulk load or long period of real-time ingestion), it can cause Spark jobs to fail with `BindException` or `OutOfMemoryError`.

To fix this, run the manual compaction job:

```bash
# Run Compaction for Weather Data
kubectl run compact-weather-job --rm -it --image=dummystad/bigdata:prediction-service-latest --restart=Never -n <your-namespace> -- python3 /app/SparkML/compact_files.py weather_raw_avro
```

This script will:
1.  Read all small files in `weather_raw_avro`.
2.  Coalesce them into fewer, larger files.
3.  Replace the original data with the compacted version.

### Repairing Hive Tables (Missing Data)
If Spark jobs report `No observations found` or `Count: 0` but you can see files in HDFS, the Hive Metastore might be out of sync with the HDFS partitions.

To fix this, run a manual repair:

```bash
# Run MSCK REPAIR TABLE manually
kubectl run repair-manual --rm -it --image=dummystad/bigdata:prediction-service-latest --restart=Never -n <your-namespace> --env="HIVE_METASTORE_URI=thrift://hive-metastore:9083" -- /bin/bash

# Inside the pod:
python3 -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.config('spark.hadoop.hive.metastore.uris', 'thrift://hive-metastore:9083').enableHiveSupport().getOrCreate(); spark.sql('MSCK REPAIR TABLE weather_raw_avro'); print('Repaired'); print('Count:', spark.sql('SELECT count(*) FROM weather_raw_avro').collect()[0][0])"
```

## Troubleshooting

*   **Job fails with `Queue full`:** The Replay Job is pushing too fast. It should be fixed in the latest image/config.
*   **Job fails with `OutOfMemory`:** The training job needs ~6GB+ RAM. Ensure your nodes have capacity.
*   **`field is immutable` error:** You cannot change a Job's spec while it exists. Delete the job first: `kubectl delete job <job-name> -n <namespace>`.
