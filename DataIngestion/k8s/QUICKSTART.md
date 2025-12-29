# Quick Start Guide - Kubernetes Data Fetching

This guide gets you up and running with parallel data fetching in Kubernetes in under 5 minutes.

## What This Does

- Fetches **DMI weather data** (2021-2025) from 57 stations with 47 parameters
- Fetches **Energy production data** (2021-2025) from all Danish municipalities
- Runs **5 pods in parallel** (one per year) for maximum speed
- Saves everything to **Parquet files** on shared storage

## Quick Deploy (3 Steps)

### 1. Deploy to Kubernetes

```bash
cd k8s
./deploy.sh
```

This will:
- Build the Docker image
- Deploy ConfigMap, PersistentVolumeClaim, and 5 Jobs
- Start fetching data immediately

### 2. Monitor Progress

```bash
./monitor.sh
```

Interactive menu with options to:
- View job status
- Check logs for specific years
- Auto-refresh status
- Check data files

Or use manual commands:
```bash
# Watch job completion
kubectl get jobs -l app=data-fetch -w

# View logs for all years
kubectl logs -l app=data-fetch --all-containers=true -f

# Check specific year
kubectl logs -l year=2021 -f
```

### 3. Download the Data

Once all jobs show "1/1" completions:

```bash
# Create temporary pod to access data
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: data-downloader
spec:
  containers:
  - name: downloader
    image: busybox
    command: ["sleep", "3600"]
    volumeMounts:
    - name: data-storage
      mountPath: /data
  volumes:
  - name: data-storage
    persistentVolumeClaim:
      claimName: weather-data-pvc
EOF

# Wait for pod
kubectl wait --for=condition=Ready pod/data-downloader

# Copy all data files
kubectl cp data-downloader:/data ../data -c downloader

# Clean up
kubectl delete pod data-downloader
```

## What You Get

After completion, you'll have 10 Parquet files in the `data/` directory:

**DMI Weather Data:**
- `dmi_weather_2021.parquet`
- `dmi_weather_2022.parquet`
- `dmi_weather_2023.parquet`
- `dmi_weather_2024.parquet`
- `dmi_weather_2025.parquet`

**Energy Production Data:**
- `energy_production_2021.parquet`
- `energy_production_2022.parquet`
- `energy_production_2023.parquet`
- `energy_production_2024.parquet`
- `energy_production_2025.parquet`

## Combine Files (Optional)

If you want single combined files:

```bash
python combine_yearly_data.py
```

This creates:
- `weather_data_all.parquet` - All DMI weather data
- `energy_production_all.parquet` - All energy data

## Expected Timeline

- **Deployment**: < 1 minute
- **Data fetching**: 30-60 minutes (all years in parallel)
- **Download**: 2-5 minutes

Total: **~40-70 minutes** vs. **2.5-5 hours** sequential!

## Verify Everything Worked

```bash
# Check all jobs completed
kubectl get jobs -l app=data-fetch

# Expected output:
# NAME              COMPLETIONS   DURATION   AGE
# data-fetch-2021   1/1           35m        40m
# data-fetch-2022   1/1           32m        40m
# data-fetch-2023   1/1           38m        40m
# data-fetch-2024   1/1           42m        40m
# data-fetch-2025   1/1           15m        40m

# List data files
kubectl exec -it data-downloader -- ls -lh /data/

# Should see 10 .parquet files
```

## Troubleshooting

### Jobs stuck in Pending

```bash
kubectl describe pod -l app=data-fetch
```

Check for:
- Insufficient resources
- PVC binding issues
- Image pull errors

### One job failed

```bash
# Check logs
kubectl logs -l year=2021

# Restart just that job
kubectl delete job data-fetch-2021
kubectl apply -f data-fetch-jobs.yaml
```

### Clean start

```bash
# Delete everything
kubectl delete -f data-fetch-jobs.yaml

# Wait a moment
sleep 5

# Re-deploy
./deploy.sh
```

## Next Steps

1. **Analyze the data** with pandas, Spark, or your preferred tool
2. **Join datasets** - weather and energy data by timestamp
3. **Build models** using the historical data
4. **Clean up Kubernetes** when done:
   ```bash
   kubectl delete -f data-fetch-jobs.yaml
   ```

## Resource Usage

Per job:
- Memory: 2Gi request, 4Gi limit
- CPU: 500m request, 2000m limit

Total (5 jobs):
- Memory: ~10-20Gi
- CPU: ~2.5-10 cores

Adjust in `data-fetch-jobs.yaml` if needed.

## Questions?

See full documentation in [README.md](README.md)
