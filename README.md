# Real-Time Energy Prediction System
**Danish Weather & Energy Data Pipeline with 10-Minute ML Predictions**

A complete real-time system that fetches weather and energy data every 10 minutes, trains ML models, and generates energy consumption predictions for Denmark's DK1 and DK2 price areas.

---

## 📊 System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────────┤
│  DMI Weather API          │      Energinet API                      │
│  (141 stations)           │      (Consumption Data)                 │
└────────┬──────────────────┴──────────────┬───────────────────────────┘
         │                                 │
         v                                 v
┌─────────────────────┐          ┌──────────────────────────┐
│ Weather Fetcher     │          │ Energy Actual Fetcher    │
│ (every 10min)       │          │ (hourly, 3-day lag)      │
└────────┬────────────┘          └────────┬─────────────────┘
         │                                │
         v                                v
┌─────────────────────────────────────────────────────────────────────┐
│                     KAFKA + SCHEMA REGISTRY                          │
├─────────────────────────────────────────────────────────────────────┤
│  weather_raw (Avro)       │      energy_actual                      │
└────────┬──────────────────┴──────────────┬───────────────────────────┘
         │                                 │
         v                                 v
┌────────────────┐                  ┌─────────────┐
│ Kafka Connect  │                  │ Kafka       │
│ HDFS Sink      │                  │ Connect     │
└────────┬───────┘                  └──────┬──────┘
         v                                 v
┌────────────────┐                  ┌─────────────┐
│ HDFS/Hive      │                  │ HDFS        │
│ (Parquet)      │                  │ (archive)   │
└────────┬───────┘                  └─────────────┘
         v
┌──────────────────────────────┐
│ Aggregate & Predict Service  │
│ (PySpark 4.1.0, Hive)        │
│ - Aggregates 10-min buckets  │
│ - Runs ML predictions        │
└────────┬─────────────────────┘
         │
         v
┌─────────────────────────────────────────────────────────────────────┐
│                    KAFKA OUTPUT TOPIC                                │
├─────────────────────────────────────────────────────────────────────┤
│  energy_predictions (6 predictions/hour)                             │
└────────┬────────────────────────────────────────────────────────────┘
         │
         v
┌────────────────┐
│ Backend+Frontend│
│ (WebSocket)     │
└─────────────────┘
```

---

## 🎯 Quick Start

### Prerequisites
- Kubernetes cluster with namespace `bd-bd-gr-08`
- HDFS (NameNode + 3 DataNodes)
- Hive Metastore + HiveServer2
- Kafka with Zookeeper
- Kafka Schema Registry
- Kafka Connect

### Deploy Core Infrastructure
```bash
# HDFS
kubectl apply -f k8s/namenode.yaml
kubectl apply -f k8s/datanodes.yaml

# Hive
kubectl apply -f k8s/hive-metastore.yaml
kubectl apply -f k8s/hive.yaml

# Kafka
kubectl apply -f k8s/kafka-connect.yaml
kubectl apply -f k8s/schema-registry.yaml
kubectl apply -f k8s/redpanda.yaml
kubectl apply -f k8s/topics.yaml
```

### Deploy Real-Time Services
```bash
# Weather fetcher (every 10 minutes, publishes Avro to Kafka)
kubectl apply -f k8s/realtime-weather-fetcher.yaml

# Energy fetcher (hourly, 3-day lag)
kubectl apply -f k8s/realtime-energy-actual-fetcher.yaml

# Aggregate & Predict service (aggregates 10-min weather, makes predictions)
kubectl apply -f k8s/aggregate-and-predict-deployment.yaml

# Backend (WebSocket server)
kubectl apply -f k8s/backend-deployment-direct.yaml

# Frontend (React UI)
kubectl apply -f k8s/frontend-deployment-direct.yaml
```

### Monitor Services
```bash
# Check all services
kubectl get pods -n bd-bd-gr-08

# Watch aggregate & predict service (shows predictions every 10 min)
kubectl logs -n bd-bd-gr-08 -l app=aggregate-and-predict -f

# Watch weather fetcher
kubectl logs -n bd-bd-gr-08 -l app=realtime-weather-fetcher -f
```

---

## 📁 Project Structure

```
BigData/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── port-forward.sh              # Port forwarding script
│
├── docs/                        # Documentation
│   ├── 10MIN_PREDICTION_SYSTEM.md
│   ├── COLUMN_SCHEMA.md
│   ├── MIGRATION_TO_10MIN.md
│   └── QUICK_START_10MIN.md
│
├── scripts/                     # Data fetching scripts
│   ├── fetch_historical_weather.py    # Historical weather (2021→now)
│   ├── fetch_historical_energy.py     # Historical energy (2021→now)
│   ├── fetch_realtime_weather.py      # Real-time weather → Kafka (Avro)
│   └── fetch_realtime_energy.py       # Real-time energy → Kafka
│
├── SparkML/                     # Machine Learning
│   ├── aggregate_and_predict_service.py  # 10-min aggregation + prediction
│   ├── train_consumption_model.py        # Train Random Forest model
│   └── models/                           # Trained models directory
│       └── energy_consumption_model_20251229_173121/
│
├── backend/                     # WebSocket backend
│   ├── server.js               # Node.js WebSocket server
│   ├── package.json
│   └── Dockerfile
│
├── frontend/                    # React frontend
│   ├── src/App.js              # React app with real-time chart
│   ├── package.json
│   └── Dockerfile
│
└── k8s/                         # Kubernetes deployments
    ├── Infrastructure
    │   ├── namenode.yaml
    │   ├── datanodes.yaml
    │   ├── hive-metastore.yaml
    │   ├── hive.yaml
    │   ├── kafka-connect.yaml
    │   ├── schema-registry.yaml
    │   └── redpanda.yaml
    │
    └── Services
        ├── realtime-weather-fetcher.yaml
        ├── realtime-energy-actual-fetcher.yaml
        ├── aggregate-and-predict-deployment.yaml
        ├── backend-deployment-direct.yaml
        └── frontend-deployment-direct.yaml
```

---

## 🔄 Data Flow

### Real-Time Pipeline (Every 10 Minutes)

```
1. Weather Fetcher (every 10 min)
   ├─ Fetches from 141 DMI stations
   ├─ Converts value field to float
   └─> Kafka: weather_raw (Avro format)
        └─> Kafka Connect → HDFS (Parquet) → Hive Table (weather_observations_raw)

2. Aggregate & Predict Service (every 10 min, with 30-min delay)
   ├─ Reads from weather_observations_raw (Hive)
   ├─ Aggregates last 10-minute bucket (30 min ago) to DK1/DK2
   ├─ Calculates weather averages (temp, wind, humidity, pressure, etc.)
   ├─ Writes to weather_area_10min (Hive)
   ├─ Loads consumption ML model (PySpark 4.1.0)
   ├─ Makes predictions for DK1 and DK2
   └─> Kafka: energy_predictions
        └─> Backend → Frontend (WebSocket)

3. Energy Actual Fetcher (every hour, 3 days back)
   ├─ Fetches consumption from Energinet
   └─> Kafka: energy_actual
        └─> Backend → Frontend (for comparison)
```

### Prediction Timing Example
```
Current time: 02:00
Latest observation: 02:00
Aggregate bucket: 01:30 (30 minutes ago)
  → Aggregates observations from 01:30:00 to 01:39:59
  → Makes prediction for 01:30
  → Publishes to Kafka
```

---

## 📋 Kafka Topics

### Input Topics

**`weather_raw`** (Avro with Schema Registry)
- **Source**: Real-time weather fetcher
- **Frequency**: Every 10 minutes
- **Consumers**: Kafka Connect → HDFS → Hive
- **Schema**:
  - `station_id` (string)
  - `observed` (string, ISO timestamp)
  - `parameter_id` (string)
  - `value` (double)

**`energy_actual`**
- **Source**: Real-time energy fetcher
- **Frequency**: Hourly (data is 3 days old)
- **Schema**:
  - `timestamp`, `dk_area`, `year`, `month`, `day`, `hour`
  - `consumption_mwh`

### Output Topic

**`energy_predictions`**
- **Source**: Aggregate & Predict service
- **Frequency**: Every 10 minutes (6 predictions/hour)
- **Format**:
```json
{
  "dk_area": "DK1",
  "timestamp": "2026-01-02T01:30:00Z",
  "year": 2026,
  "month": 1,
  "day": 2,
  "hour": 1,
  "minute_bucket": 30,
  "value": 2404.42,
  "predictions": {
    "consumption_mwh": 2404.42,
    "production_mwh": 0,
    "net_balance_mwh": 0
  },
  "model": "consumption",
  "prediction_time": "2026-01-02T01:33:45.123456"
}
```

---

## 🎓 Machine Learning Model

### Model Architecture
- **Algorithm**: Random Forest Regressor (PySpark MLlib)
- **Framework**: PySpark 4.1.0
- **Features**: 20 weather features + 3 temporal features
- **Target**: Energy consumption (MWh)
- **Model Path**: `/app/SparkML/models/energy_consumption_model_20251229_173121`

### Pipeline Stages
1. **Imputer**: Handles missing values (mean imputation)
2. **VectorAssembler**: Combines 23 features
3. **StandardScaler**: Normalizes features (mean=0, std=1)
4. **RandomForestRegressor**: 20 trees

### Features Used

**Weather Features (20)**:
- `temp_mean_area`, `temp_max_area`, `temp_min_area`
- `temp_grass_mean_area`, `temp_soil_mean_area` (imputed as NULL)
- `wind_speed_mean_area`, `wind_speed_max_area`
- `wind_dir_sin_area`, `wind_dir_cos_area`
- `wind_gust_always_past1h_max_area` (imputed as NULL)
- `radia_glob_past1h_area`, `sun_last1h_glob_area` (both imputed as NULL)
- `sun_last10min_glob_area`
- `precip_past1h_mean_area`, `precip_past10min_mean_area`
- `humidity_mean_area`
- `pressure_at_sea_mean_area`
- `cloud_cover_mean_area`
- `visibility_mean_area`
- `n_stations`

**Temporal Features (3)**:
- `month` (1-12)
- `day` (1-31)
- `hour` (0-23)

### Model Behavior
- Trained on hourly historical data
- Applied to 10-minute buckets
- Predictions remain stable within the same hour (expected behavior)
- Temporal features (month, day, hour) drive most variation
- Small weather changes within an hour don't significantly affect predictions

---

## 🗄️ Hive Tables

### `weather_observations_raw`
- **Source**: Kafka Connect from `weather_raw` topic
- **Format**: Parquet (partitioned by year/month)
- **Location**: `/topics/weather_raw/year=*/month=*/`
- **Schema**: `station_id`, `observed`, `parameter_id`, `value`
- **Update**: Every 10 minutes via Kafka Connect

### `weather_area_10min`
- **Purpose**: Aggregated 10-minute weather by DK area
- **Format**: Parquet (partitioned by year/month)
- **Schema**: See "Features Used" above + `predicted` flag
- **Update**: Every 10 minutes by aggregate-and-predict service
- **Special Column**: `predicted` (0=not predicted, increments when used)

### `predicted_buckets`
- **Purpose**: Track which buckets have been predicted
- **Schema**: `dk_area`, `year`, `month`, `day`, `hour`, `minute_bucket`, `predicted_at`
- **Update**: After each prediction

---

## 🚀 Deployment Guide

### Access Frontend
```bash
# Port forward to access locally
./port-forward.sh

# Or manually:
kubectl port-forward -n bd-bd-gr-08 svc/frontend-service 8080:80
kubectl port-forward -n bd-bd-gr-08 svc/backend-service 8081:8080

# Open browser: http://localhost:8080
```

### View Real-Time Predictions
The frontend shows a live chart with:
- Blue line: Actual consumption (3-day delayed data)
- Green line: Predicted consumption (10-minute updates)
- Updates automatically via WebSocket

### Check Kafka Topics
```bash
# List topics
kubectl exec -n bd-bd-gr-08 kafka-0 -- sh -c \
  '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list'

# Check weather_raw (Avro - will show binary)
kubectl exec -n bd-bd-gr-08 kafka-0 -- sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh \
   --bootstrap-server localhost:9092 \
   --topic weather_raw \
   --max-messages 1'

# Check predictions (JSON)
kubectl exec -n bd-bd-gr-08 kafka-0 -- sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh \
   --bootstrap-server localhost:9092 \
   --topic energy_predictions \
   --from-beginning \
   --max-messages 5'
```

### Check Hive Tables
```bash
# Count observations
kubectl exec -n bd-bd-gr-08 namenode-0 -- \
  beeline -u jdbc:hive2://hiveserver2:10000 \
  -e "SELECT COUNT(*) FROM weather_observations_raw;"

# View latest 10-min aggregations
kubectl exec -n bd-bd-gr-08 namenode-0 -- \
  beeline -u jdbc:hive2://hiveserver2:10000 \
  -e "SELECT dk_area, hour, minute_bucket, temp_mean_area, wind_speed_mean_area, predicted
      FROM weather_area_10min
      WHERE year=2026 AND month=1 AND day=2
      ORDER BY hour DESC, minute_bucket DESC
      LIMIT 10;"

# Check predictions tracking
kubectl exec -n bd-bd-gr-08 namenode-0 -- \
  beeline -u jdbc:hive2://hiveserver2:10000 \
  -e "SELECT * FROM predicted_buckets ORDER BY predicted_at DESC LIMIT 10;"
```

---

## 🛠️ Maintenance

### Restart Services
```bash
kubectl rollout restart deployment/realtime-weather-fetcher -n bd-bd-gr-08
kubectl rollout restart deployment/aggregate-and-predict-service -n bd-bd-gr-08
kubectl rollout restart deployment/backend -n bd-bd-gr-08
kubectl rollout restart deployment/frontend -n bd-bd-gr-08
```

### View Logs
```bash
# Aggregate & Predict (shows predictions every 10 min)
kubectl logs -n bd-bd-gr-08 -l app=aggregate-and-predict -f --tail=100

# Weather Fetcher
kubectl logs -n bd-bd-gr-08 -l app=realtime-weather-fetcher -f --tail=100

# Backend (WebSocket)
kubectl logs -n bd-bd-gr-08 -l app=backend -f --tail=100
```

### Check HDFS Storage
```bash
# HDFS usage
kubectl exec -n bd-bd-gr-08 namenode-0 -- \
  hdfs dfs -du -h /topics/weather_raw/ | head -20

# Partition structure
kubectl exec -n bd-bd-gr-08 namenode-0 -- \
  hdfs dfs -ls /topics/weather_raw/year=2026/month=1/
```

---

## 🐛 Troubleshooting

### No Predictions Generated

**Check 1**: Is weather data flowing to Kafka?
```bash
kubectl logs -n bd-bd-gr-08 -l app=realtime-weather-fetcher --tail=20
# Should see: "✓ Published 141 weather observations to Kafka"
```

**Check 2**: Is Kafka Connect writing to HDFS?
```bash
kubectl logs -n bd-bd-gr-08 -l app=kafka-connect --tail=50 | grep weather_raw
```

**Check 3**: Can aggregate service read from Hive?
```bash
kubectl logs -n bd-bd-gr-08 -l app=aggregate-and-predict --tail=50
# Should see: "✓ Found X new weather records"
```

**Check 4**: Is the model loaded?
```bash
kubectl logs -n bd-bd-gr-08 -l app=aggregate-and-predict | grep -i "model"
# Should see: Model loaded from /app/SparkML/models/...
```

### Avro Deserialization Errors

**Symptom**: Redpanda Console shows "issues deserializing the value"

**Fix**: Ensure `fetch_realtime_weather.py` converts `value` to float:
```python
value = float(props.get('value'))
```

### Frontend Shows 0 Values

**Check 1**: Are predictions in Kafka?
```bash
kubectl exec -n bd-bd-gr-08 kafka-0 -- sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh \
   --bootstrap-server localhost:9092 \
   --topic energy_predictions \
   --max-messages 1'
```

**Check 2**: Is backend consuming?
```bash
kubectl logs -n bd-bd-gr-08 -l app=backend --tail=50
```

**Check 3**: Is WebSocket connected?
Open browser console on http://localhost:8080 and check for WebSocket messages.

---

## 📈 Timeline & Frequency

| Time | Service | Action |
|------|---------|--------|
| Every 10 min | Weather Fetcher | Fetch from DMI, publish Avro to Kafka |
| Every 10 min | Kafka Connect | Write Parquet to HDFS |
| Every 10 min | Aggregate & Predict | Aggregate bucket from 30 min ago, predict, publish |
| Every hour | Energy Actual Fetcher | Fetch consumption (3 days back), publish to Kafka |
| Continuous | Backend | Consume from Kafka, send via WebSocket |
| Continuous | Frontend | Display real-time chart |

### Data Completeness Strategy
- Aggregate bucket from **30 minutes ago** to ensure all 10-minute observations have arrived
- Example: At 02:00, aggregate 01:30-01:39 observations
- Prevents missing data due to API delays or network issues

---

## 🔑 Key Features

✅ **High Frequency**: 6 predictions per hour (every 10 minutes)
✅ **Real-Time**: Live updates via WebSocket to frontend
✅ **Data Quality**: 30-minute delay ensures complete aggregations
✅ **Avro Schema**: Type-safe weather data with Schema Registry
✅ **Historical Archive**: All data stored in HDFS/Hive for analysis
✅ **Scalable**: Kafka-based architecture supports multiple consumers
✅ **Production-Ready**: Kubernetes deployments with auto-restart
✅ **Modern Stack**: PySpark 4.1.0, React, Node.js, Kafka

---

## 📝 Data Sources

### DMI (Danish Meteorological Institute)
- **API**: `https://dmigw.govcloud.dk/v2/metObs/collections/observation/items`
- **Stations**: 141 active stations (95 DK1, 46 DK2)
- **Parameters**: 17 weather parameters (temp, wind, humidity, etc.)
- **Frequency**: Every 10 minutes

### Energinet (Danish Energy Data Service)
- **API**: `https://api.energidataservice.dk/dataset/ConsumptionDE35Hour`
- **Coverage**: DK1 and DK2 price areas
- **Frequency**: Hourly (with 3-day reporting lag)

---

## 🤝 Contributing

For questions or issues:
1. Check logs: `kubectl logs -n bd-bd-gr-08 -l app=<service-name>`
2. Verify infrastructure: `kubectl get pods -n bd-bd-gr-08`
3. Review documentation in `docs/` folder

---

**Last Updated**: January 2, 2026
**System Status**: Production Ready ✅
**Prediction Frequency**: 6 per hour (every 10 minutes)
