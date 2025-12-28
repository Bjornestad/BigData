# Frontend Deployment Guide

Real-time power consumption monitoring and forecasting visualization system integrated with your energy prediction pipeline.

## Overview

The frontend displays real-time energy predictions and actual consumption data from your Kafka streams in a sleek industrial control-room interface.

## Architecture

```
┌─────────────────────┐         ┌──────────────┐         ┌──────────────┐
│   Kafka Topics      │────────▶│   Backend    │────────▶│   Frontend   │
│ - energy_actual     │         │  (Node.js)   │         │   (React)    │
│ - energy_predictions│         │  WebSocket   │         │              │
└─────────────────────┘         └──────────────┘         └──────────────┘
                                 Port: 8080                Port: 80
```

## Components

### Backend Service (Node.js)
- **Location**: `backend/`
- **Purpose**: Kafka consumer that streams data to frontend via WebSocket
- **Features**:
  - Consumes from `energy_actual` and `energy_predictions` topics
  - WebSocket server for real-time data streaming
  - Health check endpoint at `/health`
  - Data endpoint at `/data`
  - In-memory buffer (last 100 points)

### Frontend Application (React)
- **Location**: `frontend/`
- **Purpose**: Real-time visualization dashboard
- **Features**:
  - Interactive time-series charts with Recharts
  - Real-time updates via WebSocket
  - Industrial control-room aesthetic
  - Statistics dashboard (current load, forecast, variance)

## Quick Start

### Prerequisites
- Kubernetes cluster running in namespace `bd-bd-gr-08`
- Kafka cluster with topics: `energy_actual`, `energy_predictions`
- Prediction service already deployed and generating predictions

### Step 1: Build Docker Images

```bash
# Build backend
cd backend
docker build -t bigdata-backend:latest .

# Build frontend
cd ../frontend
docker build -t bigdata-frontend:latest .

# For remote cluster, tag and push to your registry
# docker tag bigdata-backend:latest your-registry/bigdata-backend:latest
# docker push your-registry/bigdata-backend:latest
```

### Step 2: Update Image References

Edit `k8s/backend-deployment.yaml` and `k8s/frontend-deployment.yaml` to use your images:

```yaml
spec:
  containers:
  - name: backend
    image: your-registry/bigdata-backend:latest  # Update this
    imagePullPolicy: Always
```

### Step 3: Deploy to Kubernetes

```bash
# Deploy ConfigMap
kubectl apply -f k8s/configmap.yaml

# Deploy backend
kubectl apply -f k8s/backend-deployment.yaml

# Deploy frontend
kubectl apply -f k8s/frontend-deployment.yaml

# Optional: Deploy ingress
kubectl apply -f k8s/ingress.yaml
```

### Step 4: Verify Deployment

```bash
# Check pods
kubectl get pods -n bd-bd-gr-08 | grep -E "backend|frontend"

# Should see:
# backend-xxx    1/1     Running
# frontend-xxx   1/1     Running

# Check services
kubectl get svc -n bd-bd-gr-08 | grep -E "backend|frontend"

# View backend logs
kubectl logs -n bd-bd-gr-08 -l app=backend -f

# View frontend logs
kubectl logs -n bd-bd-gr-08 -l app=frontend -f
```

### Step 5: Access the Application

**Option 1: Port Forward (for testing)**
```bash
kubectl port-forward -n bd-bd-gr-08 svc/frontend-service 3000:80
kubectl port-forward -n bd-bd-gr-08 svc/backend-service 8080:8080

# Access at http://localhost:3000
```

**Option 2: Ingress (for production)**
```bash
# Add to /etc/hosts
echo "127.0.0.1 power-grid-monitor.local" | sudo tee -a /etc/hosts

# Access via http://power-grid-monitor.local
```

**Option 3: LoadBalancer**
```bash
# Get external IP
kubectl get svc frontend-service -n bd-bd-gr-08

# Access via the EXTERNAL-IP
```

## Data Format

The backend has been updated to work with your existing Kafka topics:

### Energy Actual Topic (`energy_actual`)
```json
{
  "timestamp": "2025-12-28T10:00:00.000Z",
  "dk_area": "DK1",
  "year": 2025,
  "month": 12,
  "day": 28,
  "hour": 10,
  "total_production_mwh": 1200.50,
  "total_consumption_mwh": 4800.75,
  "net_balance_mwh": -3600.25,
  "SolarMWh": 150.0,
  "OnshoreWindMWh": 800.0,
  "OffshoreWindLt100MW_MWh": 150.0,
  "OffshoreWindGe100MW_MWh": 100.5
}
```

### Energy Predictions Topic (`energy_predictions`)
```json
{
  "timestamp": "2025-12-28T10:05:00.000Z",
  "dk_area": "DK1",
  "year": 2025,
  "month": 12,
  "day": 28,
  "hour": 10,
  "predictions": {
    "production_mwh": 1250.75,
    "consumption_mwh": 4890.20,
    "net_balance_mwh": -3639.45
  }
}
```

The backend extracts `total_consumption_mwh` for actual data and `predictions.consumption_mwh` for predicted data to display in the chart.

## Configuration

### Backend Environment Variables

Configured in `k8s/configmap.yaml`:

| Variable | Value | Description |
|----------|-------|-------------|
| `KAFKA_BROKERS` | `kafka-bootstrap:9092` | Your Kafka broker address |
| `ACTUAL_TOPIC` | `energy_actual` | Topic for actual consumption |
| `PREDICTED_TOPIC` | `energy_predictions` | Topic for ML predictions |
| `CONSUMER_GROUP` | `power-grid-monitor` | Kafka consumer group ID |
| `PORT` | `8080` | Backend HTTP/WebSocket port |

### Frontend Environment Variables

The frontend connects to the backend WebSocket at `ws://backend-service:8080` (internal cluster service).

## Monitoring

### Health Check

```bash
kubectl port-forward -n bd-bd-gr-08 svc/backend-service 8080:8080

# Check health
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "actualDataPoints": 45,
  "predictedDataPoints": 98
}
```

### View Current Data Buffer

```bash
curl http://localhost:8080/data
```

### Backend Logs

```bash
# Watch for Kafka messages
kubectl logs -n bd-bd-gr-08 -l app=backend -f

# Should see:
# Connected to Kafka
# Subscribed to topics: energy_actual, energy_predictions
# Actual: { timestamp: ..., value: 4890.20, production: 1250.75, dk_area: 'DK1', type: 'actual' }
# Predicted: { timestamp: ..., value: 4890.20, production: 1250.75, ... }
```

## Scaling

### Horizontal Scaling

```bash
# Scale backend (multiple consumers in same group)
kubectl scale deployment backend -n bd-bd-gr-08 --replicas=2

# Scale frontend
kubectl scale deployment frontend -n bd-bd-gr-08 --replicas=2
```

### Resource Limits

Adjust in deployment YAML if needed:
```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

## Troubleshooting

### Backend not connecting to Kafka

1. Check Kafka broker address in ConfigMap
2. Verify network connectivity:
   ```bash
   kubectl exec -n bd-bd-gr-08 -it <backend-pod> -- sh
   ping kafka-bootstrap
   ```
3. Check if topics exist:
   ```bash
   kubectl exec -n bd-bd-gr-08 kafka-0 -- \
     /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

### Frontend not receiving data

1. Check WebSocket connection in browser console (F12)
2. Verify backend is running:
   ```bash
   kubectl get pods -n bd-bd-gr-08 -l app=backend
   ```
3. Check backend logs:
   ```bash
   kubectl logs -n bd-bd-gr-08 -l app=backend
   ```
4. Test backend health:
   ```bash
   kubectl exec -n bd-bd-gr-08 -it <frontend-pod> -- wget -O- http://backend-service:8080/health
   ```

### No data displayed

1. Verify prediction service is generating data:
   ```bash
   kubectl logs -n bd-bd-gr-08 -l app=prediction-service-hive | grep "Published prediction"
   ```

2. Check if Kafka topics have data:
   ```bash
   kubectl exec -n bd-bd-gr-08 kafka-0 -- \
     /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic energy_predictions \
     --max-messages 1
   ```

3. Verify backend is consuming:
   ```bash
   kubectl logs -n bd-bd-gr-08 -l app=backend | grep -E "Actual:|Predicted:"
   ```

### WebSocket Connection Failed

1. Check if backend service is accessible:
   ```bash
   kubectl get svc backend-service -n bd-bd-gr-08
   ```

2. Port forward and test locally:
   ```bash
   kubectl port-forward -n bd-bd-gr-08 svc/backend-service 8080:8080

   # In another terminal, test WebSocket
   wscat -c ws://localhost:8080
   ```

3. Check backend logs for connection errors:
   ```bash
   kubectl logs -n bd-bd-gr-08 -l app=backend | grep -i error
   ```

## Integration with Existing Pipeline

The frontend integrates seamlessly with your existing real-time prediction pipeline:

```
Weather Fetcher → Kafka (weather_hourly_ml) → HDFS → Hive
                                                        ↓
Energy Fetcher → Kafka (energy_actual) ─────────────→ Backend → Frontend
                                                        ↑
Prediction Service → Kafka (energy_predictions) ──────┘
```

The backend consumes from:
- `energy_actual` - Shows actual historical consumption (3 days delayed due to Energinet lag)
- `energy_predictions` - Shows real-time ML predictions (updated every 5 minutes)

## Local Development

For local development with Docker Compose, see `docker-compose.yml`:

```bash
# Start all services (requires local Kafka)
docker-compose up --build

# Access at http://localhost:3000
```

## Next Steps

1. **Deploy the frontend** to visualize your predictions
2. **Monitor the dashboard** to see real-time predictions vs actual consumption
3. **Scale as needed** based on your traffic requirements
4. **Configure ingress** for external access

---

**Related Documentation**:
- Main README: [README.md](README.md)
- Quick Start: [QUICKSTART.md](QUICKSTART.md)
