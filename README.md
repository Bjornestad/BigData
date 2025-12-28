# Real-Time Energy Prediction System
**Danish Weather & Energy Data Pipeline with Machine Learning**

A complete real-time system that fetches weather and energy data, trains ML models, and generates hourly energy production/consumption predictions for Denmark's DK1 and DK2 price areas.

---

## 📊 System Architecture

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
│ (hourly, 141 stn)   │          │ (hourly, 3-day lag)      │
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
┌────────────────┐                  ┌─────────────┐
│ HDFS           │                  │ HDFS        │
│ (JSON files)   │                  │ (archive)   │
└────────┬───────┘                  └─────────────┘
         v
┌────────────────┐
│ Hive Tables    │
│ (external)     │
└────────┬───────┘
         v
┌─────────────────────┐
│ Prediction Service  │
│ (SparkML, Hive)     │
└────────┬────────────┘
         │
         v
┌─────────────────────────────────────────────────────────────────────┐
│                    KAFKA OUTPUT TOPIC                                │
├─────────────────────────────────────────────────────────────────────┤
│  energy_predictions (ML predictions)                                 │
└────────┬────────────────────────────────────────────────────────────┘
         │
         v
┌────────────────┐
│   FRONTEND     │
│  (Your App)    │
└────────────────┘
```

---

## 🎯 Quick Start

### Prerequisites
- Kubernetes cluster with namespace `bd-bd-gr-08`
- HDFS (NameNode + DataNodes)
- Hive Metastore
- Kafka cluster
- Kafka Connect
- Python 3.11+ with dependencies

### Step 1: Deploy Infrastructure
```bash
# HDFS
kubectl apply -f k8s/namenode.yaml
kubectl apply -f k8s/datanodes.yaml

# Hive
kubectl apply -f k8s/hive-metastore.yaml

# Kafka
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/kafka-connect.yaml
```

### Step 2: Set Up Data Pipeline
```bash
# Configure Kafka→HDFS connectors
kubectl apply -f k8s/setup-hdfs-sinks.yaml

# Create Hive tables
kubectl apply -f k8s/create-realtime-hive-tables.yaml
kubectl apply -f k8s/create-station-metadata-table.yaml
kubectl apply -f k8s/create-municipality-metadata-table.yaml
```

### Step 3: Deploy Real-Time Services
```bash
# Weather fetcher (hourly)
kubectl apply -f k8s/realtime-weather-fetcher.yaml

# Energy fetcher (hourly, 3-day lag)
kubectl apply -f k8s/realtime-energy-actual-fetcher.yaml

# Prediction service
kubectl apply -f k8s/prediction-service-hive.yaml
```

### Step 4: Monitor
```bash
# Check all services
kubectl get pods -n bd-bd-gr-08 | grep -E 'realtime|prediction'

# Watch weather fetcher
kubectl logs -n bd-bd-gr-08 -l app=realtime-weather-fetcher -f

# Watch prediction service
kubectl logs -n bd-bd-gr-08 -l app=prediction-service-hive -f
```

---

## 📁 Project Structure

```
BigData/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
│
├── scripts/                     # Data fetching scripts
│   ├── fetch_historical_weather.py    # Historical weather (2021→now)
│   ├── fetch_historical_energy.py     # Historical energy (2021→now)
│   ├── fetch_realtime_weather.py      # Real-time weather → Kafka
│   └── fetch_realtime_energy.py       # Real-time energy → Kafka
│
├── SparkML/                     # Machine Learning
│   ├── train_model.py           # Train Random Forest models
│   ├── predict_from_hive.py     # Hive-based prediction service
│   └── models/                  # Trained models directory
│       ├── energy_model_production_20251227_235847/
│       └── energy_model_consumption_20251227_235915/
│
└── k8s/                         # Kubernetes deployments
    ├── Infrastructure (5 files)
    │   ├── namenode.yaml
    │   ├── datanodes.yaml
    │   ├── hive-metastore.yaml
    │   ├── kafka.yaml
    │   └── kafka-connect.yaml
    │
    ├── Production Services (5 files)
    │   ├── realtime-weather-fetcher.yaml
    │   ├── realtime-energy-actual-fetcher.yaml
    │   ├── prediction-service-hive.yaml
    │   ├── setup-hdfs-sinks.yaml
    │   └── create-realtime-hive-tables.yaml
    │
    └── Metadata (2 files)
        ├── create-station-metadata-table.yaml
        └── create-municipality-metadata-table.yaml
```

---

## 🔄 Data Flow

### Historical Data (One-Time Training)
```
1. Run fetch_historical_weather.py
   └─> Output: data/historical_weather_ml.parquet

2. Run fetch_historical_energy.py
   └─> Output: data/historical_energy_ml.parquet

3. Load into Hive tables (manually or via job)

4. Train ML models: SparkML/train_model.py
   └─> Output: SparkML/models/energy_model_production_*
   └─> Output: SparkML/models/energy_model_consumption_*
```

### Real-Time Data (Continuous Production)
```
Every Hour:
├─ Weather Fetcher
│  ├─ Fetch from 141 DMI stations
│  ├─ Aggregate to DK1/DK2
│  └─> Kafka: weather_hourly_ml
│       └─> Kafka Connect → HDFS → Hive Table (weather_wind_solar_area_hourly)
│
├─ Energy Fetcher (3 days back due to lag)
│  ├─ Fetch production (ProductionConsumptionSettlement)
│  ├─ Fetch consumption (ConsumptionDE35Hour)
│  └─> Kafka: energy_actual
│       └─> Kafka Connect → HDFS (archive)
│
└─ Prediction Service (checks every 5 min)
   ├─ Read latest weather from Hive
   ├─ Run SparkML models
   └─> Kafka: energy_predictions
       └─> Frontend consumes
```

---

## 📋 Kafka Topics

### Input Topics

**`weather_hourly_ml`**
- **Source**: Real-time weather fetcher
- **Frequency**: Hourly
- **Consumers**: Kafka Connect (→HDFS→Hive), Prediction Service (via Hive)
- **Schema**:
  - `timestamp`, `dk_area`, `year`, `month`, `day`, `hour`
  - `wind_speed_mean_area`, `wind_speed_max_area`
  - `wind_dir_sin_area`, `wind_dir_cos_area`
  - `radia_glob_past1h_area`, `sun_last1h_glob_area`
  - `cloud_cover_mean_area`, `temperature_avg`, `humidity_avg`
  - `n_stations_wind`, `n_stations_solar`

**`energy_actual`**
- **Source**: Real-time energy fetcher
- **Frequency**: Hourly (data is 3 days old due to Energinet lag)
- **Consumers**: Kafka Connect (→HDFS for archive)
- **Schema**:
  - `timestamp`, `dk_area`, `year`, `month`, `day`, `hour`
  - `total_production_mwh`, `total_consumption_mwh`
  - `net_balance_mwh`
  - `SolarMWh`, `OnshoreWindMWh`, `OffshoreWindLt100MW_MWh`, `OffshoreWindGe100MW_MWh`

### Output Topic

**`energy_predictions`**
- **Source**: Prediction service
- **Frequency**: Every 5 minutes (when new data detected)
- **Format**:
```json
{
  "timestamp": "2025-12-28T10:05:00.000000",
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

---

## 🎓 Machine Learning Models

### Model Architecture
- **Algorithm**: Random Forest Regressor
- **Features**: 10 weather + 2 temporal features
- **Targets**: Production (MWh), Consumption (MWh)
- **Training Data**: 86,160 hourly records (2021-2025)

### Performance Metrics

**Production Model**:
- R² = 0.76 (76% variance explained)
- RMSE = 576.51 MWh
- MAE = 428.70 MWh

**Consumption Model**:
- R² = 0.89 (89% variance explained)
- RMSE = 489.04 MWh
- MAE = 362.22 MWh

### Features Used

1. **Weather Features** (10):
   - `wind_speed_mean_area`, `wind_speed_max_area`
   - `wind_dir_sin_area`, `wind_dir_cos_area`
   - `radia_glob_past1h_area`, `sun_last1h_glob_area`
   - `cloud_cover_mean_area`
   - `temperature_avg`, `humidity_avg`
   - `n_stations_wind`, `n_stations_solar`

2. **Temporal Features** (2):
   - `month_of_year` (1-12)
   - `hour_of_day` (0-23)

---

## 📦 Data Fetching Scripts

### Historical Scripts (One-Time)

**`fetch_historical_weather.py`**
- Fetches from 141 DMI weather stations (95 DK1, 46 DK2)
- Date range: 2021-01-01 to present
- Output: `data/historical_weather_ml.parquet`
- Runtime: ~30-60 minutes

**`fetch_historical_energy.py`**
- Fetches production and consumption from Energinet
- Date range: 2021-01-01 to present
- Output: `data/historical_energy_ml.parquet`
- Runtime: ~5-10 minutes

### Real-Time Scripts (Continuous)

**`fetch_realtime_weather.py`**
- Deployed as Kubernetes service
- Fetches every 1 hour (3600s)
- Publishes to Kafka topic `weather_hourly_ml`
- Data flows: Kafka → HDFS → Hive → SparkML

**`fetch_realtime_energy.py`**
- Deployed as Kubernetes service
- Fetches every 1 hour (3600s), **from 3 days ago** (Energinet lag)
- Publishes to Kafka topic `energy_actual`
- Used for validation and comparison with predictions

---

## 🚀 Deployment Guide

### Verify Prerequisites
```bash
# Check infrastructure
kubectl get pods -n bd-bd-gr-08 | grep -E 'kafka|hdfs|hive'

# Should see:
# - kafka-0, kafka-1, kafka-2
# - namenode-0, datanode-*
# - hive-metastore-0
# - kafka-connect-*
```

### Check Kafka Connect Sinks
```bash
kubectl exec -n bd-bd-gr-08 $(kubectl get pod -n bd-bd-gr-08 -l app=kafka-connect -o name | head -1) -- \
  curl -s http://localhost:8083/connectors

# Should show:
# ["hdfs-sink-weather-ml", "hdfs-sink-energy-actual", "hdfs-sink-energy-predictions"]
```

### Verify Hive Tables
```bash
kubectl exec -n bd-bd-gr-08 hive-metastore-0 -- \
  hive -e "SHOW TABLES;" | grep -E 'weather|energy'

# Should show:
# weather_wind_solar_area_hourly
# energy_actual_realtime
# energy_predictions_realtime_raw
```

### Deploy Services
```bash
# 1. Weather fetcher
kubectl apply -f k8s/realtime-weather-fetcher.yaml
kubectl logs -n bd-bd-gr-08 -l app=realtime-weather-fetcher -f

# 2. Energy fetcher
kubectl apply -f k8s/realtime-energy-actual-fetcher.yaml
kubectl logs -n bd-bd-gr-08 -l app=realtime-energy-actual-fetcher -f

# 3. Wait 5 minutes for data to flow to Hive

# 4. Prediction service
kubectl apply -f k8s/prediction-service-hive.yaml
kubectl logs -n bd-bd-gr-08 -l app=prediction-service-hive -f
```

### Test End-to-End
```bash
# 1. Check weather in Kafka
kubectl exec -n bd-bd-gr-08 kafka-0 -- sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh \
   --bootstrap-server localhost:9092 \
   --topic weather_hourly_ml \
   --max-messages 1'

# 2. Check predictions in Kafka
kubectl exec -n bd-bd-gr-08 kafka-0 -- sh -c \
  '/opt/kafka/bin/kafka-console-consumer.sh \
   --bootstrap-server localhost:9092 \
   --topic energy_predictions \
   --max-messages 1'

# 3. Check weather in Hive
kubectl exec -n bd-bd-gr-08 hive-metastore-0 -- \
  hive -e "SELECT COUNT(*) FROM weather_wind_solar_area_hourly;"
```

---

## 🛠️ Maintenance

### Restart Services
```bash
kubectl rollout restart deployment realtime-weather-fetcher -n bd-bd-gr-08
kubectl rollout restart deployment realtime-energy-actual-fetcher -n bd-bd-gr-08
kubectl rollout restart deployment prediction-service-hive -n bd-bd-gr-08
```

### View Logs
```bash
kubectl logs -n bd-bd-gr-08 -l app=realtime-weather-fetcher -f --tail=100
kubectl logs -n bd-bd-gr-08 -l app=realtime-energy-actual-fetcher -f --tail=100
kubectl logs -n bd-bd-gr-08 -l app=prediction-service-hive -f --tail=100
```

### Check Data Volume
```bash
# HDFS usage
kubectl exec -n bd-bd-gr-08 namenode-0 -- \
  hdfs dfs -du -h /user/hive/warehouse/ | grep -E 'weather|energy'

# Hive row counts
kubectl exec -n bd-bd-gr-08 hive-metastore-0 -- \
  hive -e "
  SELECT 'weather', COUNT(*) FROM weather_wind_solar_area_hourly
  UNION ALL
  SELECT 'actual', COUNT(*) FROM energy_actual_realtime
  UNION ALL
  SELECT 'predictions', COUNT(*) FROM energy_predictions_realtime_raw;
  "
```

---

## 🐛 Troubleshooting

### No Predictions Generated

**Check 1**: Is weather data in Hive?
```bash
kubectl exec -n bd-bd-gr-08 hive-metastore-0 -- \
  hive -e "SELECT COUNT(*) FROM weather_wind_solar_area_hourly;"
```
If 0: Check weather fetcher and Kafka Connect

**Check 2**: Are models loaded?
```bash
kubectl logs -n bd-bd-gr-08 -l app=prediction-service-hive | grep -i "model loaded"
# Should show: ✓ Production model loaded, ✓ Consumption model loaded
```

**Check 3**: Can prediction service read from Hive?
```bash
kubectl logs -n bd-bd-gr-08 -l app=prediction-service-hive | grep -A5 "Reading latest"
```

### Kafka Connect Not Writing to HDFS

```bash
# Check connector status
kubectl exec -n bd-bd-gr-08 $(kubectl get pod -n bd-bd-gr-08 -l app=kafka-connect -o name | head -1) -- \
  curl -s http://localhost:8083/connectors/hdfs-sink-weather-ml/status | jq .

# Check for errors
kubectl logs -n bd-bd-gr-08 -l app=kafka-connect --tail=100 | grep -i error
```

### Services Not Starting

```bash
# Check pod status
kubectl get pods -n bd-bd-gr-08 | grep -E 'realtime|prediction'

# Describe pod for details
kubectl describe pod -n bd-bd-gr-08 <pod-name>

# Check events
kubectl get events -n bd-bd-gr-08 --sort-by='.lastTimestamp'
```

---

## 💻 Frontend Integration

### Consuming Predictions

**JavaScript (KafkaJS)**:
```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  brokers: ['kafka-bootstrap:9092']
});

const consumer = kafka.consumer({ groupId: 'frontend-group' });

await consumer.connect();
await consumer.subscribe({ topic: 'energy_predictions' });

await consumer.run({
  eachMessage: async ({ topic, message }) => {
    const prediction = JSON.parse(message.value.toString());
    console.log(`${prediction.dk_area} ${prediction.hour}:00`);
    console.log(`  Production: ${prediction.predictions.production_mwh} MWh`);
    console.log(`  Consumption: ${prediction.predictions.consumption_mwh} MWh`);
    console.log(`  Net: ${prediction.predictions.net_balance_mwh} MWh`);

    // Update your UI here
    updateDashboard(prediction);
  },
});
```

**Python (kafka-python)**:
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'energy_predictions',
    bootstrap_servers=['kafka-bootstrap:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    prediction = message.value
    print(f"{prediction['dk_area']} {prediction['hour']}:00")
    print(f"  Production: {prediction['predictions']['production_mwh']} MWh")
    print(f"  Consumption: {prediction['predictions']['consumption_mwh']} MWh")
```

---

## 📚 Data Sources

### DMI (Danish Meteorological Institute)
- **API**: `https://dmigw.govcloud.dk/v2/metObs/collections/observation/items`
- **Stations**: 141 active stations (95 DK1, 46 DK2)
- **Parameters**: Wind speed/direction, solar radiation, cloud cover, temperature, humidity
- **Frequency**: Hourly observations

### Energinet (Danish Energy Data Service)
- **API**: `https://api.energidataservice.dk/dataset/`
- **Datasets**:
  - `ProductionConsumptionSettlement` - Production by municipality
  - `ConsumptionDE35Hour` - Consumption by price area
- **Coverage**: All Danish municipalities
- **Frequency**: Hourly (with 3-day reporting lag)
- **Note**: Real-time fetcher automatically fetches from 3 days ago

---

## 📈 Timeline & Frequency

| Time | Service | Action |
|------|---------|--------|
| Every hour (T+0) | Weather Fetcher | Fetch from DMI, publish to Kafka |
| Every hour (T+0) | Energy Actual Fetcher | Fetch from Energinet (3 days back), publish to Kafka |
| T+1 min | Kafka Connect | Write to HDFS |
| T+1 min | Hive | Data available (external table) |
| T+2-5 min | Prediction Service | Read from Hive, make predictions, publish to Kafka |
| Continuous | Frontend | Consume predictions from Kafka in real-time |

---

## 🔑 Key Features

✅ **Real-Time**: Hourly weather updates and predictions
✅ **Historical Archive**: All data stored in HDFS for analysis
✅ **High Accuracy**: 76% R² for production, 89% R² for consumption
✅ **Scalable**: Kafka-based architecture supports multiple consumers
✅ **Production-Ready**: Kubernetes deployments with auto-restart
✅ **Comprehensive**: Weather + Energy in one unified system
✅ **Clean Output**: Predictions contain only essential data (no weather details)

---

## 📝 License

This project uses data from:
- Danish Meteorological Institute (DMI)
- Energinet (Danish Energy Data Service)

Please review their respective terms of service for data usage.

---

## 🤝 Contributing

For questions or issues:
1. Check logs: `kubectl logs -n bd-bd-gr-08 -l app=<service-name>`
2. Verify infrastructure: `kubectl get pods -n bd-bd-gr-08`
3. Review this README for deployment and troubleshooting guides

---

**Last Updated**: December 28, 2025
**System Status**: Production Ready ✅
