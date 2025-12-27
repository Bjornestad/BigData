# Hive Tables Documentation

## Summary
**Total Tables:** 10 (9 tables + 1 view)

---

## 1. `path_test` ⭐ (Streaming Test Table)
**Purpose:** Real-time streaming test - receives data from Kafka Connect
**Format:** JSON (TextFile with JsonSerDe)
**Location:** `hdfs://namenode:9000/user/hive/warehouse/path_test/weather_raw/partition=0`

### Schema:
```sql
CREATE EXTERNAL TABLE path_test (
  message STRING,
  iteration INT,
  test BOOLEAN,
  id INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/path_test/weather_raw/partition=0';
```

### Pipeline:
**Producer (Python) → Kafka topic `weather_raw` → Kafka Connect HDFS Sink → HDFS → Hive table `path_test`**

---

## 2. `weather_observations_raw`
**Purpose:** Raw weather observations in long format (parameter-value pairs)
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/weather_raw`
**Rows:** 198,866,490 observations

### Schema:
```sql
CREATE EXTERNAL TABLE weather_observations_raw (
  station_id STRING,
  station_name STRING,
  ts STRING,                -- timestamp as string
  parameter_id STRING,       -- e.g., 'temp_dry', 'humidity', 'wind_speed'
  value DOUBLE,
  latitude DOUBLE,
  longitude DOUBLE,
  year INT,
  month INT,
  day INT,
  hour INT,
  minute INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/weather_raw';
```

### Data Format:
Long format - one row per observation:
```
station_id | parameter_id | value | timestamp
06080      | temp_dry     | 15.2  | 2021-01-01 00:00
06080      | humidity     | 82.0  | 2021-01-01 00:00
06080      | wind_speed   | 5.1   | 2021-01-01 00:00
```

---

## 3. `weather_wide`
**Purpose:** Weather data pivoted to wide format (all parameters as columns)
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/weather_wide`
**Parameters:** 52 weather parameters as columns

### Schema:
```sql
CREATE TABLE weather_wide (
  station_id STRING,
  station_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  year INT, month INT, day INT, hour INT, minute INT,

  -- Temperature parameters (15)
  temp_dry DOUBLE,
  temp_dew DOUBLE,
  temp_mean_past1h DOUBLE,
  temp_max_past1h DOUBLE,
  temp_min_past1h DOUBLE,
  temp_min_past12h DOUBLE,
  temp_max_past12h DOUBLE,
  temp_grass DOUBLE,
  temp_grass_mean_past1h DOUBLE,
  temp_grass_min_past1h DOUBLE,
  temp_grass_max_past1h DOUBLE,
  temp_soil DOUBLE,
  temp_soil_mean_past1h DOUBLE,
  temp_soil_max_past1h DOUBLE,
  temp_soil_min_past1h DOUBLE,

  -- Humidity parameters (2)
  humidity DOUBLE,
  humidity_past1h DOUBLE,

  -- Pressure parameters (2)
  pressure DOUBLE,
  pressure_at_sea DOUBLE,

  -- Wind parameters (11)
  wind_dir DOUBLE,
  wind_speed DOUBLE,
  wind_max DOUBLE,
  wind_min DOUBLE,
  wind_min_past1h DOUBLE,
  wind_speed_past1h DOUBLE,
  wind_dir_past1h DOUBLE,
  wind_gust_always_past1h DOUBLE,
  wind_max_per10min_past1h DOUBLE,

  -- Precipitation parameters (5)
  precip_past10min DOUBLE,
  precip_past1min DOUBLE,
  precip_past1h DOUBLE,
  precip_dur_past10min DOUBLE,
  precip_dur_past1h DOUBLE,

  -- Visibility parameters (2)
  visib_mean_last10min DOUBLE,
  visibility DOUBLE,

  -- Solar radiation parameters (4)
  radia_glob DOUBLE,
  radia_glob_past1h DOUBLE,
  sun_last10min_glob DOUBLE,
  sun_last1h_glob DOUBLE,

  -- Cloud parameters (2)
  cloud_cover DOUBLE,
  cloud_height DOUBLE,

  -- Other parameters (3)
  weather STRING,
  leav_hum_dur_past10min DOUBLE,
  leav_hum_dur_past1h DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/weather_wide';
```

### Creation Method:
Pivot transformation from `weather_observations_raw` using:
```sql
INSERT OVERWRITE TABLE weather_wide
SELECT
  station_id, station_name,
  MAX(latitude) as latitude,
  MAX(longitude) as longitude,
  year, month, day, hour, minute,
  MAX(CASE WHEN parameter_id = 'temp_dry' THEN value END) as temp_dry,
  MAX(CASE WHEN parameter_id = 'humidity' THEN value END) as humidity,
  -- ... for all 52 parameters
FROM weather_observations_raw
GROUP BY station_id, station_name, year, month, day, hour, minute;
```

---

## 4. `station_municipality_mapping`
**Purpose:** Maps weather stations to Danish municipalities
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/station_municipality_mapping`
**Rows:** 61 stations

### Schema:
```sql
CREATE TABLE station_municipality_mapping (
  station_id STRING,           -- e.g., '06080'
  station_name STRING,         -- e.g., 'Esbjerg Lufthavn'
  municipality_code INT,       -- e.g., 561 (Esbjerg Kommune)
  municipality_name STRING     -- e.g., 'Esbjerg'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/station_municipality_mapping';
```

---

## 5. `energy_production`
**Purpose:** Energy production by municipality (2021-2025)
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/energy_by_municipality`
**Rows:** 4,290,421 records
**Coverage:** 100 municipalities, 2021-01-01 to 2025-12-09

### Schema:
```sql
CREATE EXTERNAL TABLE energy_production (
  HourUTC STRING,                      -- UTC timestamp
  HourDK STRING,                       -- Danish timezone timestamp
  MunicipalityNo STRING,               -- Municipality code
  SolarMWh DOUBLE,                     -- Solar production (MWh)
  OffshoreWindLt100MW_MWh DOUBLE,      -- Small offshore wind
  OffshoreWindGe100MW_MWh DOUBLE,      -- Large offshore wind
  OnshoreWindMWh DOUBLE,               -- Onshore wind
  ThermalPowerMWh DOUBLE,              -- Thermal power
  ts BIGINT,                           -- Unix timestamp
  year INT,
  month INT,
  day INT,
  hour INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/energy_by_municipality';
```

### Energy Parameters Coverage:
- **SolarMWh:** 4,254,157 (99.2%)
- **OnshoreWindMWh:** 4,290,421 (100%)
- **ThermalPowerMWh:** 3,691,177 (86.0%)
- **OffshoreWindLt100MW_MWh:** 345,311 (8.0% - coastal only)
- **OffshoreWindGe100MW_MWh:** 297,768 (6.9% - coastal only)

---

## 6. `weather_energy_combined`
**Purpose:** Joined weather + energy data for ML training
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/weather_energy_combined`

### Schema:
```sql
CREATE TABLE weather_energy_combined (
  municipality_code INT,
  municipality_name STRING,
  station_id STRING,
  station_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  year INT, month INT, day INT, hour INT, minute INT,

  -- Weather features
  temp_dry DOUBLE,
  temp_dew DOUBLE,
  humidity DOUBLE,
  wind_speed DOUBLE,
  wind_dir DOUBLE,
  pressure DOUBLE,

  -- Energy production
  energy_hour_utc STRING,
  energy_hour_dk STRING,
  SolarMWh DOUBLE,
  OffshoreWindLt100MW_MWh DOUBLE,
  OffshoreWindGe100MW_MWh DOUBLE,
  OnshoreWindMWh DOUBLE,
  ThermalPowerMWh DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/weather_energy_combined';
```

### Creation Method:
```sql
INSERT OVERWRITE TABLE weather_energy_combined
SELECT
  sm.municipality_code,
  sm.municipality_name,
  w.station_id,
  w.station_name,
  AVG(w.latitude) as latitude,
  AVG(w.longitude) as longitude,
  w.year, w.month, w.day, w.hour, w.minute,
  AVG(w.temp_dry) as temp_dry,
  AVG(w.temp_dew) as temp_dew,
  AVG(w.humidity) as humidity,
  AVG(w.wind_speed) as wind_speed,
  AVG(w.wind_dir) as wind_dir,
  AVG(w.pressure) as pressure,
  e.HourUTC as energy_hour_utc,
  e.HourDK as energy_hour_dk,
  e.SolarMWh,
  e.OffshoreWindLt100MW_MWh,
  e.OffshoreWindGe100MW_MWh,
  e.OnshoreWindMWh,
  e.ThermalPowerMWh
FROM weather_wide w
JOIN station_municipality_mapping sm ON w.station_id = sm.station_id
LEFT JOIN energy_production e
  ON sm.municipality_code = CAST(e.MunicipalityNo AS INT)
  AND w.year = e.year
  AND w.month = e.month
  AND w.day = e.day
  AND w.hour = e.hour
WHERE w.minute = 0  -- Only hourly observations
GROUP BY sm.municipality_code, sm.municipality_name,
         w.station_id, w.station_name,
         w.year, w.month, w.day, w.hour, w.minute,
         e.HourUTC, e.HourDK, e.SolarMWh, ...;
```

---

## 7. `weather_hourly` (VIEW)
**Purpose:** View showing hourly weather aggregations
**Type:** Hive VIEW (not a table)

### Definition:
```sql
CREATE VIEW weather_hourly AS
SELECT
  station_id, station_name,
  year, month, day, hour,
  latitude, longitude,
  MAX(CASE WHEN parameter_id = 'temp_dry' THEN value END) as temp_dry,
  MAX(CASE WHEN parameter_id = 'temp_dew' THEN value END) as temp_dew,
  MAX(CASE WHEN parameter_id = 'temp_mean_past1h' THEN value END) as temp_mean_past1h,
  MAX(CASE WHEN parameter_id = 'humidity' THEN value END) as humidity,
  MAX(CASE WHEN parameter_id = 'pressure_at_sea' THEN value END) as pressure_at_sea,
  MAX(CASE WHEN parameter_id = 'wind_speed' THEN value END) as wind_speed,
  MAX(CASE WHEN parameter_id = 'wind_dir' THEN value END) as wind_dir,
  MAX(CASE WHEN parameter_id = 'cloud_cover' THEN value END) as cloud_cover,
  MAX(CASE WHEN parameter_id = 'precip_past1h' THEN value END) as precip_past1h
FROM weather_raw
WHERE minute = 0
GROUP BY station_id, station_name, year, month, day, hour, latitude, longitude;
```

---

## 8. `weather_raw` (Partitioned)
**Purpose:** Partitioned version of raw weather data
**Format:** Parquet (partitioned)
**Location:** `hdfs://namenode:9000/user/hive/warehouse/weather_raw`

### Schema:
```sql
CREATE EXTERNAL TABLE weather_raw (
  station_id STRING,
  station_name STRING,
  parameter_id STRING,
  value DOUBLE,
  longitude DOUBLE,
  latitude DOUBLE,
  year INT, month INT, day INT, hour INT, minute INT
)
PARTITIONED BY (dummy STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/weather_raw';
```

---

## 9. `weather`
**Purpose:** Full weather data in wide format (historical/alternative table)
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/weather`

### Schema:
```sql
CREATE EXTERNAL TABLE weather (
  station_id STRING,
  station_name STRING,
  timestamp TIMESTAMP,
  latitude DOUBLE,
  longitude DOUBLE,
  date DATE,
  year INT, month INT, day INT, hour INT, minute INT,

  -- 47 weather parameters including:
  temp_dry DOUBLE,
  temp_dew DOUBLE,
  temp_mean_past1h DOUBLE,
  temp_max_past1h DOUBLE,
  temp_min_past1h DOUBLE,
  temp_max_past12h DOUBLE,
  temp_min_past12h DOUBLE,
  temp_grass DOUBLE,
  temp_grass_min_past12h DOUBLE,
  temp_soil_10cm DOUBLE,
  temp_soil_30cm DOUBLE,
  humidity DOUBLE,
  humidity_past1h DOUBLE,
  pressure_at_sea DOUBLE,
  pressure_at_station DOUBLE,
  wind_dir DOUBLE,
  wind_speed DOUBLE,
  wind_gust_always_past1h DOUBLE,
  wind_max DOUBLE,
  wind_min DOUBLE,
  visibility DOUBLE,
  snow_depth_man DOUBLE,
  snow_cover_man DOUBLE,
  precip_past1h DOUBLE,
  precip_past6h DOUBLE,
  precip_past12h DOUBLE,
  precip_past24h DOUBLE,
  precip_dur_past1h DOUBLE,
  precip_dur_past6h DOUBLE,
  precip_dur_past12h DOUBLE,
  precip_dur_past24h DOUBLE,
  radia_glob DOUBLE,
  radia_glob_past1h DOUBLE,
  sun_last10min_glob DOUBLE,
  sun_last1h_glob DOUBLE,
  cloud_cover DOUBLE,
  cloud_height DOUBLE,
  weather_wx DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/weather';
```

---

## 10. `energy`
**Purpose:** Alternative energy table (different schema)
**Format:** Parquet
**Location:** `hdfs://namenode:9000/user/hive/warehouse/energy`

### Schema:
```sql
CREATE EXTERNAL TABLE energy (
  HourUTC TIMESTAMP,
  HourDK TIMESTAMP,
  PriceArea STRING,
  MunicipalityNo INT,
  Municipality STRING,
  ProductionGe100kW DOUBLE,
  ProductionLt100kW DOUBLE,
  Production DOUBLE,
  timestamp TIMESTAMP,
  date DATE,
  year INT, month INT, day INT, hour INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/energy';
```

---

## Data Pipeline Summary

### Historical Batch Data:
1. **Fetch raw data** → Python scripts fetch from DMI API and Energy Data Service
2. **Store as Parquet** → Local parquet files
3. **Copy to HDFS** → `hdfs dfs -put` or Kubernetes jobs
4. **Create Hive tables** → External tables pointing to HDFS locations
5. **Transform** → Pivot long to wide format, join weather + energy

### Real-time Streaming Pipeline:
1. **Producer:** [realtime_weather_to_connect.py](realtime_weather_to_connect.py) (runs in K8s pod)
2. **Kafka Topic:** `weather_raw`
3. **Kafka Connect:** HDFS Sink connector (JSON format, flush every 5 messages/30 sec)
4. **HDFS:** `/user/hive/warehouse/path_test/weather_raw/partition=0/`
5. **Hive Table:** `path_test` (JSON SerDe, queries real-time data)

---

## Key Tables for ML Training:

**Primary:** `weather_energy_combined` - Ready for ML with weather features + energy labels

**Components:**
- `weather_wide` - All 52 weather parameters in wide format
- `energy_production` - Full energy production data (2021-2025)
- `station_municipality_mapping` - Geographic join key

**Real-time Testing:** `path_test` - Validates streaming pipeline
