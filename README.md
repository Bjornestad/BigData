# Real-Time Energy Prediction System
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
│                    KAFKA OUTPUT TOPIC                               │
├─────────────────────────────────────────────────────────────────────┤
│  energy_predictions (ML predictions)                                │
└────────┬────────────────────────────────────────────────────────────┘
         │
         v
┌────────────────┐
│   BACKEND      │
│                │
└────────────────┘
         │
         v
┌────────────────┐
│   FRONTEND     │
│                │
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

### Step 2: Set Up Data Pipeline


### Step 3: Deploy Real-Time Services

### Step 4: Monitor

## Project Structure



## Data Flow

### Historical Data (One-Time Training)
```
1. Run fetch_historical_weather.py
   └─> Output: data/weather_raw_historical.parquet

2. Run fetch_historical_energy.py
   └─> Output: data/energy_actual.parquet

3. Load into Hive tables (manually or via job)

4. Train ML models: SparkML/train_model.py
   └─> Output: SparkML/models/energy_model_consumption_*
```

### Real-Time Data (Continuous Production)
```
├─ Weather Fetcher (5 mins)
│  ├─ Fetch from 141 DMI stations
│  ├─ Aggregate to DK1/DK2
│  └─> Kafka: weather_raw_avro
│       └─> Kafka Connect → HDFS → Hive Table (weather_raw)
│
├─ Energy Fetcher (Hourly)
│  ├─ Fetch production (ProductionConsumptionSettlement)
│  ├─ Fetch consumption (ConsumptionDE35Hour)
│  └─> Kafka: energy_actual
│       └─> Kafka Connect → HDFS -> Hive Table (energy_actual)
│
└─ Prediction Service (checks every 5 min)
   ├─ Read latest weather data from Hive
   ├─ Converts from long to wide format with station and timestamp as primary keys
   ├─ Aggregate all wide formatted rows into one single row pr. DK PriceArea by either taking average or max values from 23 parameters 
   ├─ Add energy lag (72 and 168 hour)
   ├─ Run SparkML models
   └─> Kafka: energy_predictions
       └─> Frontend consumes
```

---

## Kafka Topics

### Input Topics

**`weather_raw_avro`**
- **Source**: Real-time weather fetcher
- **Frequency**: Hourly
- **Consumers**: Kafka Connect (→HDFS→Hive), Prediction Service (via Hive)
- **Schema**:
  - `timestamp`, `station_id`, `observed`, `parameter_id`, `value`

**`energy_actual`**
- **Source**: Real-time energy fetcher
- **Frequency**: Hourly (data is 3 days old due to Energinet lag)
- **Consumers**: Kafka Connect (→HDFS for archive)
- **Schema**:
  - `HourUTC`, `HourDK`, `PriceArea`, `ConnectedArea`, `ViaArea`, `SharePPM`, 
  - `ShareMWh`, `Updated`, `year`, `month`, `day`



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
  "consumption_mwh": 4890.20,
  }
}
```

---

## Machine Learning Models

### Model Architecture
- **Algorithm**: Random Forest Regressor
- **Features**: 23 weather + 8 temporal features + 4 lag features
- **Targets**:  Consumption (MWh)
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

1. **Weather Features** (23):
   - `temp_dry`, `temp_dew`, `temp_grass`
   - `temp_soil`, `humidity`, `pressure`,
   - `pressure_at_sea`, `wind_dir_sin`, `wind_dir_cos`, 
   - `wind_speed`, `wind_max`, `wind_min`,
   - `precip_past10min`, `precip_dur_past10min`, `visibility`,
   - `visib_mean_last10min`, `cloud_cover`, `cloud_height`,
   - `weather`, `radia_glob`, `radia_glob_past1h`,
   - `sun_last10min_glob`, `n_stations`

2. **Temporal Features** (8):
   - `hour_sin`, `hour_cos`, `day_of_year_sin`,
   - `day_of_year_cos`, `day_of_week`, `is_weekend`,
   - `month`, `day`

3. **Lag Features** (4):
   - `load_lag_72`, `load_lag_168`,
   - `load_mean_72h`, `load_mean_7d`

---

## Data Fetching Scripts

### Historical Scripts (One-Time)

**`fetch_historical_weather.py`**
- Fetches from 141 DMI weather stations (95 DK1, 46 DK2)
- Date range: 2021-01-01 to present
- Output: `data/historical_weather_ml.parquet`

**`fetch_historical_energy.py`**
- Fetches production and consumption from Energinet
- Date range: 2021-01-01 to present
- Output: `data/historical_energy_ml.parquet`

### Real-Time Scripts (Continuous)

**`fetch_realtime_weather.py`**
- Deployed as Kubernetes service
- Fetches every 5 mins (300s)
- Publishes to Kafka topic `weather_raw`
- Data flows: Kafka → HDFS → Hive → SparkML

**`fetch_realtime_energy.py`**
- Deployed as Kubernetes service
- Fetches every 1 hour (3600s), **from 3 days ago** (Energinet lag)
- Publishes to Kafka topic `energy_actual`
- Data flows: Kafka → HDFS → Hive → SparkML

---

## Deployment Guide

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
# ["hdfs-sink-raw-avro", "hdfs-sink-energy-actual", "hdfs-sink-energy-predictions"]
```

### Verify Hive Tables
```bash
kubectl exec -n bd-bd-gr-08 hive-metastore-0 -- \
  hive -e "SHOW TABLES;" | grep -E 'weather|energy'

# Should show:
# weather_raw
# weather_area_hourly
# weather_area_10mins
# weather_station_metadata
# consumption_area_hourly
# energy_actual
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
  hive -e "SELECT COUNT(*) FROM weather_raw;"
```

---

## Data Sources

### DMI (Danish Meteorological Institute)
- **API**: `https://dmigw.govcloud.dk/v2/metObs/collections/observation/items`
- **Stations**: 141 active stations (95 DK1, 46 DK2)

### Energinet (Danish Energy Data Service)
- **API**: `https://api.energidataservice.dk/dataset/`
- **Datasets**:
  - `ConsumptionCoverageLocationBased` - Consumption by price area

---

## License

This project uses data from:
- Danish Meteorological Institute (DMI)
- Energinet (Danish Energy Data Service)

Please review their respective terms of service for data usage.

---

**Last Updated**: Januar 4, 2026
