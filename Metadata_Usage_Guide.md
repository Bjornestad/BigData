# Metadata Tables Usage Guide

This guide shows how the metadata tables are stored in Hive and how to use them in queries.

## 📊 Available Metadata Tables

### 1. **station_metadata** - Weather Station Metadata
- **Location**: `hdfs://namenode:9000/user/hive/warehouse/station_metadata/`
- **Records**: 140 Danish weather stations
- **Columns**:
  - `station_id` (STRING): 5-digit station identifier (e.g., "05005", "06180")
  - `station_name` (STRING): Human-readable station name (e.g., "Uggerby", "Københavns Lufthavn")
  - `dk_area` (STRING): Electricity price area - "DK1" or "DK2"

**Distribution**:
- DK1 (Jutland + Funen): 99 stations
- DK2 (Zealand + islands): 41 stations

### 2. **municipality_metadata** - Municipality Metadata
- **Location**: `hdfs://namenode:9000/user/hive/warehouse/municipality_metadata/`
- **Records**: 99 Danish administrative units
- **Columns**:
  - `municipality_code` (STRING): 3-digit municipality code (e.g., "101", "461", "751")
  - `municipality_name` (STRING): Municipality name (e.g., "København", "Odense", "Aarhus")
  - `dk_area` (STRING): Electricity price area - "DK1" or "DK2"

**Distribution**:
- DK1 (Jutland + Funen): 52 municipalities
- DK2 (Zealand + islands): 47 municipalities

---

## 🔍 How to Use Metadata Tables in Hive

### Example 1: Get Weather Data with Station Names and DK Areas

```sql
-- Join weather observations with station metadata
SELECT
    w.station_id,
    s.station_name,
    s.dk_area,
    w.timestamp,
    w.parameter_id,
    w.value
FROM weather_observations_raw w
LEFT JOIN station_metadata s ON w.station_id = s.station_id
WHERE w.timestamp >= '2025-01-01'
LIMIT 100;
```

### Example 2: Count Observations by DK Area

```sql
-- Count weather observations per electricity area
SELECT
    s.dk_area,
    COUNT(*) as observation_count,
    COUNT(DISTINCT w.station_id) as station_count
FROM weather_observations_raw w
LEFT JOIN station_metadata s ON w.station_id = s.station_id
GROUP BY s.dk_area
ORDER BY s.dk_area;
```

### Example 3: Energy Production by DK Area

```sql
-- Aggregate energy production by DK area
SELECT
    m.dk_area,
    SUM(e.SolarMWh) as total_solar_mwh,
    SUM(e.OnshoreWindMWh) as total_onshore_wind_mwh,
    SUM(e.OffshoreWindGe100MW_MWh) as total_offshore_wind_mwh,
    COUNT(*) as record_count
FROM energy_by_municipality e
LEFT JOIN municipality_metadata m ON e.MunicipalityNo = m.municipality_code
GROUP BY m.dk_area
ORDER BY m.dk_area;
```

### Example 4: Top Producing Municipalities with Names

```sql
-- Get top 10 solar energy producing municipalities with names
SELECT
    m.municipality_name,
    m.dk_area,
    SUM(e.SolarMWh) as total_solar_mwh
FROM energy_by_municipality e
LEFT JOIN municipality_metadata m ON e.MunicipalityNo = m.municipality_code
GROUP BY m.municipality_name, m.dk_area
ORDER BY total_solar_mwh DESC
LIMIT 10;
```

### Example 5: Weather and Energy Combined Analysis

```sql
-- Get average temperature and energy production for each DK area
-- (This would require joining weather data aggregated by area with energy data)

-- Step 1: Get average weather values by DK area and date
WITH weather_by_area AS (
    SELECT
        s.dk_area,
        DATE(w.timestamp) as date,
        AVG(CASE WHEN w.parameter_id = 'temp_dry' THEN w.value END) as avg_temperature
    FROM weather_observations_raw w
    LEFT JOIN station_metadata s ON w.station_id = s.station_id
    WHERE w.timestamp >= '2025-01-01'
    GROUP BY s.dk_area, DATE(w.timestamp)
),
-- Step 2: Get total energy by DK area and date
energy_by_area AS (
    SELECT
        m.dk_area,
        DATE(e.HourDK) as date,
        SUM(e.SolarMWh) as total_solar,
        SUM(e.OnshoreWindMWh) as total_wind
    FROM energy_by_municipality e
    LEFT JOIN municipality_metadata m ON e.MunicipalityNo = m.municipality_code
    WHERE e.HourDK >= '2025-01-01'
    GROUP BY m.dk_area, DATE(e.HourDK)
)
-- Step 3: Join weather and energy data
SELECT
    w.dk_area,
    w.date,
    w.avg_temperature,
    e.total_solar,
    e.total_wind
FROM weather_by_area w
JOIN energy_by_area e ON w.dk_area = e.dk_area AND w.date = e.date
ORDER BY w.dk_area, w.date
LIMIT 100;
```

### Example 6: List All Stations in a Specific Municipality Area

```sql
-- Find all weather stations in DK1 municipalities
SELECT
    s.station_id,
    s.station_name,
    s.dk_area,
    COUNT(*) OVER (PARTITION BY s.dk_area) as stations_in_area
FROM station_metadata s
WHERE s.dk_area = 'DK1'
ORDER BY s.station_id;
```

### Example 7: Energy Consumption with Municipality Names

```sql
-- Join consumption data with municipality metadata
SELECT
    c.MunicipalityNo,
    m.municipality_name,
    m.dk_area,
    c.HourDK,
    c.TotalCon
FROM energy_consumption c
LEFT JOIN municipality_metadata m ON c.MunicipalityNo = m.municipality_code
WHERE c.HourDK >= '2025-01-01'
LIMIT 100;
```

---

## 📋 Quick Reference - Common Patterns

### Pattern 1: Adding Human-Readable Names
```sql
-- Always use LEFT JOIN to preserve records even if metadata is missing
FROM your_table t
LEFT JOIN station_metadata s ON t.station_id = s.station_id

-- Or for municipalities:
FROM your_table t
LEFT JOIN municipality_metadata m ON t.MunicipalityNo = m.municipality_code
```

### Pattern 2: Filtering by DK Area
```sql
-- Filter for West Denmark (Jutland + Funen)
WHERE s.dk_area = 'DK1'

-- Filter for East Denmark (Zealand + islands)
WHERE m.dk_area = 'DK2'
```

### Pattern 3: Aggregating by DK Area
```sql
-- Always group by the metadata dk_area column
GROUP BY s.dk_area
-- or
GROUP BY m.dk_area
```

### Pattern 4: Using COALESCE for Fallback
```sql
-- Use COALESCE to show station_id if name is missing
SELECT COALESCE(s.station_name, w.station_id) as display_name
FROM weather_observations_raw w
LEFT JOIN station_metadata s ON w.station_id = s.station_id
```

---

## 💡 Best Practices

1. **Always use LEFT JOIN** - This ensures you don't lose data if a station/municipality is not in the metadata table
2. **Check NULL values** - Use `WHERE m.dk_area IS NOT NULL` if you want to exclude records without metadata
3. **Use aliases** - Makes queries more readable: `s` for stations, `m` for municipalities
4. **Filter early** - Apply WHERE conditions before joins when possible for better performance
5. **Index on join keys** - Hive will use partition/bucket optimization if configured

---

## 📁 File Locations

### Local Files (for reference):
- `data/station_metadata.parquet` - Weather stations with DK areas
- `data/station_names_mapping.csv` - CSV version of stations
- `data/municipality_metadata.parquet` - Municipalities with DK areas
- `data/municipality_metadata.csv` - CSV version of municipalities

### HDFS Locations:
- `hdfs://namenode:9000/user/hive/warehouse/station_metadata/`
- `hdfs://namenode:9000/user/hive/warehouse/municipality_metadata/`

### Hive Tables:
```sql
-- View table definitions
DESCRIBE station_metadata;
DESCRIBE municipality_metadata;

-- View table locations
SHOW CREATE TABLE station_metadata;
SHOW CREATE TABLE municipality_metadata;
```

---

## 🔄 Refreshing Metadata

If you need to update the metadata tables:

```bash
# Update station metadata
kubectl apply -f k8s/update-station-metadata.yaml

# Update municipality metadata
kubectl apply -f k8s/create-municipality-metadata-table.yaml
```

Then refresh the Hive tables:
```sql
-- Drop and recreate if schema changed
DROP TABLE station_metadata;
CREATE EXTERNAL TABLE station_metadata (
  station_id STRING,
  station_name STRING,
  dk_area STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/station_metadata';
```
