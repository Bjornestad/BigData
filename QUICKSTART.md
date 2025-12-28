# Quick Start Guide

Get the Power Grid Monitor up and running in 5 minutes!

## Prerequisites

- Docker Desktop installed
- Docker Compose installed
- At least 4GB of free RAM

## Start Everything with Test Data

```bash
# 1. Navigate to the project directory
cd power-grid-monitor

# 2. Start all services including test data producer
docker-compose --profile with-test-data up --build

# 3. Wait for services to start (about 30-60 seconds)

# 4. Open your browser to http://localhost:3000
```

That's it! You should see:
- ⚡ A live graph showing green (actual) and purple (predicted) power consumption
- 📊 Real-time statistics updating every 2 seconds
- 🔴 A "LIVE" indicator showing the connection status

## Start Without Test Data

If you want to connect to your own Kafka cluster:

```bash
# 1. Update backend environment in docker-compose.yml
#    Set KAFKA_BROKERS to your broker address

# 2. Start without test producer
docker-compose up --build backend frontend

# 3. Send data to these Kafka topics:
#    - power-consumption-actual
#    - power-consumption-predicted
```

## Stopping the Application

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f frontend
docker-compose logs -f test-producer
```

## Troubleshooting

### "Can't connect to backend"
- Wait 30 seconds for Kafka to fully start
- Check backend logs: `docker-compose logs backend`
- Verify Kafka is running: `docker-compose ps kafka`

### "No data showing"
- Make sure you started with `--profile with-test-data`
- Or send data to Kafka topics manually
- Check producer logs: `docker-compose logs test-producer`

### Port already in use
- Change ports in docker-compose.yml
- Or stop conflicting services

## Next Steps

- Read the full [README.md](README.md) for Kubernetes deployment
- Customize the Kafka topics and data format
- Modify the frontend styling in `frontend/src/App.css`
- Adjust the data generation in `kafka-test/producer.js`

## Data Format

Send JSON to Kafka topics:

```json
{
  "timestamp": "2024-01-15T14:30:00.000Z",
  "value": 1234.56
}
```

- `timestamp`: ISO 8601 format
- `value`: Power in megawatts (MW)

Enjoy your power grid monitoring! ⚡
