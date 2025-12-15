# Power Grid Monitor

Real-time power consumption monitoring and forecasting visualization system for Kubernetes. This application displays actual and predicted power consumption data from Kafka streams in a sleek industrial control-room interface.

## Features

- 🔴 **Real-time Data Streaming**: WebSocket-based live updates from Kafka
- 📊 **Interactive Visualization**: Beautiful time-series charts with Recharts
- 🎨 **Industrial Design**: Control-room aesthetic with glowing data lines
- 🟢 **Dual Data Display**: Green lines for actual consumption, purple for predictions
- 📈 **Live Statistics**: Current load, forecast, variance, and averages
- ⚡ **High Performance**: Optimized for low-latency updates
- 🐳 **Container-Ready**: Docker images for easy deployment
- ☸️ **Kubernetes Native**: Full K8s manifests included

## Architecture

```
┌─────────────┐         ┌──────────────┐         ┌──────────────┐
│   Kafka     │────────▶│   Backend    │────────▶│   Frontend   │
│  (Topics)   │         │  (Node.js)   │         │   (React)    │
└─────────────┘         └──────────────┘         └──────────────┘
     │                       │                         │
     │                       │                         │
  Topics:              WebSocket Server           WebSocket Client
  - actual             Port: 8080                 Port: 3000
  - predicted          
```

### Components

1. **Backend Service** (Node.js)
   - Kafka consumer for two topics (actual and predicted consumption)
   - WebSocket server for real-time data streaming
   - Health check endpoint
   - In-memory data buffering (last 100 points)

2. **Frontend Application** (React)
   - Real-time chart visualization with Recharts
   - WebSocket client for live updates
   - Responsive design with industrial aesthetic
   - Statistics dashboard

3. **Kafka Test Producer** (Optional)
   - Generates realistic test data
   - Simulates time-of-day and weekly patterns
   - Creates predictions with variance

## Quick Start

### Prerequisites

- Docker and Docker Compose (for local development)
- Kubernetes cluster (for production deployment)
- Kafka cluster (with bootstrap servers accessible)

### Local Development with Docker Compose

1. **Clone the repository**
   ```bash
   cd power-grid-monitor
   ```

2. **Create a docker-compose.yml** (example below)

3. **Build and run**
   ```bash
   docker-compose up --build
   ```

4. **Access the application**
   - Frontend: http://localhost:3000
   - Backend: http://localhost:8080

### Example Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  backend:
    build: ./backend
    ports:
      - "8080:8080"
    environment:
      KAFKA_BROKERS: kafka:29092
      ACTUAL_TOPIC: power-consumption-actual
      PREDICTED_TOPIC: power-consumption-predicted
      CONSUMER_GROUP: power-grid-monitor
    depends_on:
      - kafka

  frontend:
    build: ./frontend
    ports:
      - "3000:3000"
    environment:
      REACT_APP_BACKEND_URL: ws://localhost:8080
    depends_on:
      - backend

  test-producer:
    build: ./kafka-test
    environment:
      KAFKA_BROKERS: kafka:29092
      ACTUAL_TOPIC: power-consumption-actual
      PREDICTED_TOPIC: power-consumption-predicted
      INTERVAL_MS: 2000
    depends_on:
      - kafka
```

## Kubernetes Deployment

### 1. Build Docker Images

```bash
# Build backend
cd backend
docker build -t power-grid-backend:latest .

# Build frontend
cd ../frontend
docker build -t power-grid-frontend:latest .

# Tag and push to your registry (replace with your registry)
docker tag power-grid-backend:latest your-registry/power-grid-backend:latest
docker push your-registry/power-grid-backend:latest

docker tag power-grid-frontend:latest your-registry/power-grid-frontend:latest
docker push your-registry/power-grid-frontend:latest
```

### 2. Update Configuration

Edit `k8s/configmap.yaml` to point to your Kafka brokers:

```yaml
data:
  KAFKA_BROKERS: "your-kafka-broker:9092"
```

### 3. Deploy to Kubernetes

```bash
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Apply configuration
kubectl apply -f k8s/configmap.yaml

# Deploy backend
kubectl apply -f k8s/backend-deployment.yaml

# Deploy frontend
kubectl apply -f k8s/frontend-deployment.yaml

# Optional: Create ingress
kubectl apply -f k8s/ingress.yaml
```

### 4. Verify Deployment

```bash
# Check pod status
kubectl get pods -n power-grid-monitor

# Check services
kubectl get svc -n power-grid-monitor

# View logs
kubectl logs -f -n power-grid-monitor -l app=backend
kubectl logs -f -n power-grid-monitor -l app=frontend
```

### 5. Access the Application

If using LoadBalancer:
```bash
kubectl get svc frontend-service -n power-grid-monitor
# Access via the EXTERNAL-IP
```

If using Ingress:
```bash
# Add to /etc/hosts if using local domain
echo "127.0.0.1 power-grid-monitor.local" | sudo tee -a /etc/hosts

# Access via http://power-grid-monitor.local
```

If using port-forward (for testing):
```bash
kubectl port-forward -n power-grid-monitor svc/frontend-service 3000:80
kubectl port-forward -n power-grid-monitor svc/backend-service 8080:8080
# Access via http://localhost:3000
```

## Kafka Data Format

The application expects JSON messages in the following format:

### Actual Consumption Topic
```json
{
  "timestamp": "2024-01-15T14:30:00.000Z",
  "value": 1234.56
}
```

### Predicted Consumption Topic
```json
{
  "timestamp": "2024-01-15T15:00:00.000Z",
  "value": 1250.78
}
```

Where:
- `timestamp`: ISO 8601 format datetime
- `value`: Power consumption in megawatts (MW)

## Configuration

### Backend Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Comma-separated Kafka broker addresses | `localhost:9092` |
| `ACTUAL_TOPIC` | Kafka topic for actual consumption | `power-consumption-actual` |
| `PREDICTED_TOPIC` | Kafka topic for predicted consumption | `power-consumption-predicted` |
| `CONSUMER_GROUP` | Kafka consumer group ID | `power-grid-monitor` |
| `PORT` | Backend HTTP/WebSocket port | `8080` |

### Frontend Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REACT_APP_BACKEND_URL` | Backend WebSocket URL | `ws://localhost:8080` |

## Testing with Sample Data

Use the included test producer to generate realistic sample data:

```bash
cd kafka-test
npm install
KAFKA_BROKERS=localhost:9092 npm start
```

This will:
- Generate actual consumption data with time-of-day patterns
- Create predictions for the next 30 minutes
- Simulate realistic variations and spikes
- Send data every 2 seconds

## Monitoring

### Health Checks

Backend health endpoint:
```bash
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

### Data Endpoint

View current data buffer:
```bash
curl http://localhost:8080/data
```

## Scaling

### Horizontal Scaling

Backend (multiple consumers in same group):
```bash
kubectl scale deployment backend -n power-grid-monitor --replicas=3
```

Frontend (multiple instances):
```bash
kubectl scale deployment frontend -n power-grid-monitor --replicas=3
```

### Resource Limits

Adjust in deployment YAML:
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

1. Check Kafka broker addresses in ConfigMap
2. Verify network connectivity: `kubectl exec -it <backend-pod> -n power-grid-monitor -- ping kafka-broker`
3. Check Kafka broker logs
4. Verify topics exist: `kafka-topics.sh --list --bootstrap-server kafka:9092`

### Frontend not receiving data

1. Check WebSocket connection in browser console
2. Verify backend is running: `kubectl get pods -n power-grid-monitor`
3. Check backend logs: `kubectl logs -f <backend-pod> -n power-grid-monitor`
4. Verify REACT_APP_BACKEND_URL is correct

### No data displayed

1. Verify Kafka topics have data: `kafka-console-consumer.sh --topic power-consumption-actual --bootstrap-server kafka:9092`
2. Check backend is consuming: Look for "Actual:" and "Predicted:" in logs
3. Verify WebSocket connections: Check "Client connected" in backend logs

## Performance Optimization

### Backend
- Adjust `MAX_DATA_POINTS` in `server.js` to control memory usage
- Use Kafka consumer group for distributed load
- Enable compression for Kafka messages

### Frontend
- Data is automatically limited to last 100 points
- Chart uses `isAnimationActive={false}` for better performance
- Consider debouncing updates for very high-frequency data

## Security Considerations

For production deployments:

1. **Use TLS/SSL**
   - Enable HTTPS for frontend
   - Use WSS (WebSocket Secure) instead of WS
   - Configure Kafka SSL/SASL

2. **Authentication**
   - Add API authentication to backend
   - Implement OAuth/JWT for frontend
   - Use Kafka ACLs

3. **Network Policies**
   - Restrict pod-to-pod communication
   - Use Kubernetes Network Policies
   - Implement firewall rules

4. **Secrets Management**
   - Use Kubernetes Secrets for sensitive data
   - Consider using external secret management (Vault, etc.)

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Support

For issues and questions:
- Check the troubleshooting section
- Review backend logs
- Check Kafka broker connectivity
- Verify WebSocket connection in browser console
