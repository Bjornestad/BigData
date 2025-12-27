# Consumption Coverage Location-Based Table

## Overview

The `consumption_coverage_location` table contains hourly data showing **where electricity consumed in Denmark comes from** - whether it's local production, imports from neighboring countries, or inter-Denmark transfers.

## Table Information

- **Table Name**: `consumption_coverage_location`
- **Location**: `hdfs://namenode:9000/user/hive/warehouse/consumption_coverage_location`
- **Format**: Parquet (Snappy compression)
- **Records**: 680,444 hourly records
- **Date Range**: 2021-01-01 to 2025-11-30 (nearly 5 years)
- **Source**: Energy Data Service API - ConsumptionCoverageLocationBased

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `HourUTC` | STRING | Hour timestamp in UTC |
| `HourDK` | STRING | Hour timestamp in Danish time |
| `PriceArea` | STRING | Consuming price area (DK1 or DK2) |
| `ConnectedArea` | STRING | Source region/country of electricity |
| `ViaArea` | STRING | Transmission route/connection point |
| `SharePPM` | INT | Share in parts per million (proportion) |
| `ShareMWh` | DOUBLE | Actual energy share in megawatt-hours |
| `Updated` | STRING | Last data update timestamp |
| `timestamp` | TIMESTAMP | Parsed timestamp for queries |
| `date` | STRING | Date extracted from timestamp |
| `hour` | INT | Hour of day (0-23) |
| `year` | INT | Year |
| `month` | INT | Month (1-12) |
| `day` | INT | Day of month |
| `weekday` | INT | Day of week (0=Monday, 6=Sunday) |

## Connected Areas (Source Regions)

The `ConnectedArea` field indicates where the electricity comes from:

- **DK1**: West Denmark (Jutland + Funen) - local production
- **DK2**: East Denmark (Zealand + islands) - local production
- **GE**: Germany
- **GB**: Great Britain (via interconnector)
- **NL**: Netherlands
- **NO2**: Norway zone 2
- **SE**: Sweden

## Key Concepts

### Energy Flow
- When `PriceArea = DK1` and `ConnectedArea = DK1`: Electricity consumed in West Denmark from local West Denmark production
- When `PriceArea = DK1` and `ConnectedArea = NO2`: Electricity consumed in West Denmark imported from Norway
- When `PriceArea = DK1` and `ConnectedArea = DK2`: Electricity consumed in West Denmark transferred from East Denmark

### Shares
- **SharePPM**: Proportion in parts per million (1,000,000 PPM = 100%)
- **ShareMWh**: Actual energy amount in megawatt-hours

## Usage Examples

### Example 1: Total Consumption by Source for DK1

```sql
SELECT
  year,
  month,
  ConnectedArea,
  SUM(ShareMWh) as total_mwh,
  COUNT(*) as hours
FROM consumption_coverage_location
WHERE PriceArea = 'DK1'
GROUP BY year, month, ConnectedArea
ORDER BY year DESC, month DESC, total_mwh DESC;
```

### Example 2: Import Dependency Over Time

```sql
-- Calculate percentage of consumption from imports vs local production
SELECT
  year,
  month,
  PriceArea,
  SUM(CASE WHEN ConnectedArea = PriceArea THEN ShareMWh ELSE 0 END) as local_mwh,
  SUM(CASE WHEN ConnectedArea != PriceArea THEN ShareMWh ELSE 0 END) as import_mwh,
  ROUND(
    100.0 * SUM(CASE WHEN ConnectedArea != PriceArea THEN ShareMWh ELSE 0 END) /
    NULLIF(SUM(ShareMWh), 0),
    2
  ) as import_percentage
FROM consumption_coverage_location
GROUP BY year, month, PriceArea
ORDER BY year DESC, month DESC, PriceArea;
```

### Example 3: Hourly Import Pattern (Daily Average)

```sql
-- Average hourly import pattern by hour of day
SELECT
  hour,
  PriceArea,
  AVG(CASE WHEN ConnectedArea = PriceArea THEN ShareMWh ELSE 0 END) as avg_local_mwh,
  AVG(CASE WHEN ConnectedArea != PriceArea THEN ShareMWh ELSE 0 END) as avg_import_mwh
FROM consumption_coverage_location
WHERE year = 2025 AND month = 11
GROUP BY hour, PriceArea
ORDER BY PriceArea, hour;
```

### Example 4: Top Import Sources by Month

```sql
-- Which countries Denmark imports most from each month
SELECT
  year,
  month,
  PriceArea,
  ConnectedArea,
  SUM(ShareMWh) as total_imports_mwh,
  ROUND(AVG(SharePPM) / 10000.0, 2) as avg_share_pct
FROM consumption_coverage_location
WHERE ConnectedArea NOT IN ('DK1', 'DK2')  -- Only imports
GROUP BY year, month, PriceArea, ConnectedArea
ORDER BY year DESC, month DESC, total_imports_mwh DESC
LIMIT 20;
```

### Example 5: Join with Weather Data for Analysis

```sql
-- Correlate wind conditions with import dependency
SELECT
  w.station_id,
  s.station_name,
  s.dk_area,
  DATE(w.hour_utc) as date,
  AVG(w.wind_speed_avg) as avg_wind_speed,
  AVG(CASE
    WHEN c.ConnectedArea = c.PriceArea THEN c.ShareMWh
    ELSE 0
  END) as local_production_share,
  AVG(CASE
    WHEN c.ConnectedArea != c.PriceArea THEN c.ShareMWh
    ELSE 0
  END) as import_share
FROM weather_station_hourly w
JOIN station_metadata s ON w.station_id = s.station_id
JOIN consumption_coverage_location c
  ON DATE(w.hour_utc) = c.`date` AND s.dk_area = c.PriceArea
WHERE w.year = 2025 AND w.month = 11
GROUP BY w.station_id, s.station_name, s.dk_area, DATE(w.hour_utc)
ORDER BY date DESC, station_id
LIMIT 50;
```

### Example 6: Inter-Denmark Energy Exchange

```sql
-- How much energy flows between DK1 and DK2
SELECT
  year,
  month,
  PriceArea as consuming_area,
  ConnectedArea as source_area,
  SUM(ShareMWh) as total_mwh,
  COUNT(*) as hours
FROM consumption_coverage_location
WHERE (PriceArea = 'DK1' AND ConnectedArea = 'DK2')
   OR (PriceArea = 'DK2' AND ConnectedArea = 'DK1')
GROUP BY year, month, PriceArea, ConnectedArea
ORDER BY year DESC, month DESC;
```

## Data Quality Notes

- All hourly data from 2021-01-01 to 2025-11-30
- Each hour typically has multiple records (one per connected area with non-zero share)
- SharePPM values should sum to ~1,000,000 per (PriceArea, hour) combination
- Missing or zero values indicate no energy flow from that source

## Related Tables

- **station_metadata**: Contains DK1/DK2 classification for weather stations
- **municipality_metadata**: Contains DK1/DK2 classification for municipalities
- **energy_production**: Energy production data by municipality and fuel type
- **weather_station_hourly**: Hourly weather observations (when available)

## Quick Stats Query

```sql
SELECT
  COUNT(*) as total_records,
  COUNT(DISTINCT CONCAT(year, '-', month, '-', day, '-', hour, '-', PriceArea)) as unique_hours,
  MIN(year) as earliest_year,
  MAX(year) as latest_year,
  COUNT(DISTINCT ConnectedArea) as num_source_regions,
  SUM(ShareMWh) as total_mwh
FROM consumption_coverage_location;
```

Expected output:
- Total records: ~680,000
- Unique hours: ~43,000 (5 years * 365 days * 24 hours * 2 price areas)
- Source regions: 7 (DK1, DK2, GE, GB, NL, NO2, SE)
- Total energy: ~300 million MWh

## File Locations

- **Parquet file**: `/user/hive/warehouse/consumption_coverage_location/consumption_coverage_data.parquet` (7.1 MB)
- **Local backup**: `/home/akris19/BigData/data/consumption_coverage_data.parquet`
- **Fetch script**: `/home/akris19/BigData/scripts/fetch_consumption_coverage.py`

## Maintenance

To refresh the data with latest available records:

```bash
# Run fetch script
python3 /home/akris19/BigData/scripts/fetch_consumption_coverage.py

# Upload to PVC
kubectl cp data/consumption_coverage_data.parquet bd-bd-gr-08/data-copier:/data/

# Reload in Hive
kubectl exec -n bd-bd-gr-08 hiveserver2-xxx -- beeline -u jdbc:hive2://localhost:10000 -e "
  DROP TABLE consumption_coverage_location;
  -- Recreate table (see create command above)
"
```
