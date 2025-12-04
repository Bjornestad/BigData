# Kubernetes Deployment Guide

This setup distributes data fetching across multiple Kubernetes pods, with each pod responsible for fetching one year of data from both DMI (weather) and Energy Data Service APIs.

## Architecture

- **5 Jobs** (2021, 2022, 2023, 2024, 2025) - one per year
- **Shared PersistentVolume** - all pods write to the same storage
- **Parallel Execution** - all jobs run simultaneously for maximum speed
- **Output**: Parquet files per year:
  - `dmi_weather_2021.parquet`, `dmi_weather_2022.parquet`, etc.
  - `energy_production_2021.parquet`, `energy_production_2022.parquet`, etc.

## Prerequisites

1. **Kubernetes cluster** with kubectl configured
2. **Docker image** built and available to your cluster
3. **Storage class** that supports ReadWriteMany (e.g., NFS, CephFS, Azure Files)

## Setup Steps

### 1. Build and Load Docker Image

```bash
# Build the Docker image
docker build -t bigdata-fetcher:latest .

# For Minikube (local testing)
eval $(minikube docker-env)
docker build -t bigdata-fetcher:latest .

# For remote cluster, push to registry
docker tag bigdata-fetcher:latest your-registry/bigdata-fetcher:latest
docker push your-registry/bigdata-fetcher:latest
```

### 2. Update ConfigMap (Optional)

Edit `k8s/data-fetch-jobs.yaml` and update the API_KEY if needed:

```yaml
data:
  API_KEY: "your-api-key-here"
```

### 3. Deploy to Kubernetes

```bash
# Apply all resources
kubectl apply -f k8s/data-fetch-jobs.yaml

# This creates:
# - ConfigMap with API credentials
# - PersistentVolumeClaim for shared storage
# - 5 Jobs (one per year)
```

### 4. Monitor Jobs

```bash
# Watch job status
kubectl get jobs -l app=data-fetch -w

# Check pod status
kubectl get pods -l app=data-fetch

# View logs for specific year
kubectl logs -l year=2021 -f

# View logs for all years
kubectl logs -l app=data-fetch --all-containers=true

# Get job completion status
kubectl get jobs -l app=data-fetch
```

### 5. Access the Data

Once jobs are complete, you can access the data from the PersistentVolume:

```bash
# Create a pod to access the data
kubectl run data-access --image=busybox --rm -it --restart=Never \
  --overrides='
{
  "spec": {
    "containers": [{
      "name": "data-access",
      "image": "busybox",
      "command": ["sh"],
      "stdin": true,
      "tty": true,
      "volumeMounts": [{
        "name": "data",
        "mountPath": "/data"
      }]
    }],
    "volumes": [{
      "name": "data",
      "persistentVolumeClaim": {
        "claimName": "weather-data-pvc"
      }
    }]
  }
}'

# Inside the pod, list files
ls -lh /data/
```

### 6. Copy Data to Local Machine

```bash
# Create a temporary pod with the volume mounted
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

# Wait for pod to be ready
kubectl wait --for=condition=Ready pod/data-downloader

# Copy files to local machine
kubectl cp data-downloader:/data ./data -c downloader

# Clean up
kubectl delete pod data-downloader
```

## Resource Requirements

Each job requests:
- **Memory**: 2Gi (limit: 4Gi)
- **CPU**: 500m (limit: 2000m)

Total for 5 parallel jobs:
- **Memory**: ~10Gi requested, 20Gi limit
- **CPU**: ~2.5 cores requested, 10 cores limit

Adjust these values in the YAML file based on your cluster capacity.

## Customization

### Change Years

Edit `k8s/data-fetch-jobs.yaml` and modify the `YEAR` environment variable in each job.

### Add More Years

Copy one of the job definitions and change:
1. Job name: `data-fetch-YYYY`
2. Year label: `year: "YYYY"`
3. YEAR env var: `value: "YYYY"`

### Use Different Namespace

Change `namespace: default` to your desired namespace in all resources.

### Use Remote Docker Registry

In each job spec, change:
```yaml
image: bigdata-fetcher:latest
imagePullPolicy: Never  # Change to Always or IfNotPresent
```

To:
```yaml
image: your-registry/bigdata-fetcher:latest
imagePullPolicy: Always
```

## Troubleshooting

### Jobs Not Starting

```bash
# Check events
kubectl describe job data-fetch-2021

# Check if PVC is bound
kubectl get pvc weather-data-pvc
```

### Pod Failures

```bash
# View pod logs
kubectl logs -l year=2021

# Describe pod for events
kubectl describe pod -l year=2021
```

### Storage Issues

If you get PVC binding issues:
1. Check your storage class supports ReadWriteMany
2. Use a different storage class:
```yaml
spec:
  storageClassName: your-storage-class-name
  accessModes:
    - ReadWriteMany
```

### Clean Up Failed Jobs

```bash
# Delete all jobs
kubectl delete jobs -l app=data-fetch

# Delete failed pods
kubectl delete pods -l app=data-fetch --field-selector=status.phase=Failed

# Re-apply the jobs
kubectl apply -f k8s/data-fetch-jobs.yaml
```

## Performance Tips

1. **Parallel Execution**: All 5 jobs run simultaneously by default
2. **Resource Tuning**: Increase CPU/memory limits if you have capacity
3. **Network**: Ensure good network connectivity to DMI and Energy APIs
4. **Storage**: Use fast storage (SSD-backed) for better write performance

## Expected Runtime

With default settings (parallel execution):
- **Per year**: ~30-60 minutes depending on network and API speed
- **Total**: ~30-60 minutes (all years complete around the same time)

Sequential execution would take 2.5-5 hours instead!

## Output Files

After completion, you'll have these files in `/data`:

```
dmi_weather_2021.parquet
dmi_weather_2022.parquet
dmi_weather_2023.parquet
dmi_weather_2024.parquet
dmi_weather_2025.parquet
energy_production_2021.parquet
energy_production_2022.parquet
energy_production_2023.parquet
energy_production_2024.parquet
energy_production_2025.parquet
```

## Next Steps

After fetching the data:
1. Copy data to your local machine or analysis environment
2. Combine yearly Parquet files if needed
3. Load into your preferred analysis tool (pandas, Spark, DuckDB, etc.)
4. Clean up Kubernetes resources when done
