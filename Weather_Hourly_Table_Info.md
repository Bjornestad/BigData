# Weather Station Hourly Aggregation Table

## Overview

The `weather_station_hourly` table aggregates raw weather observations into hourly records per station, making it much easier to analyze and join with energy data.

## Table Structure

### Source Data
- **Input**: `weather_observations_raw` (338+ million observations)
- **Processing**: Aggregation by station_id and hour (UTC)
- **Output**: `weather_station_hourly` (hourly aggregates)

### Columns

| Column Name | Type | Description |
|-------------|------|-------------|
| `station_id` | STRING | Weather station identifier (e.g., "05005", "06180") |
| `hour_utc` | TIMESTAMP | Hour bucket in UTC time |
| **Temperature & Humidity** |||
| `temp_dry_avg` | DOUBLE | Average dry bulb temperature (°C) |
| `temp_dew_avg` | DOUBLE | Average dew point temperature (°C) |
| `humidity_avg` | DOUBLE | Average relative humidity (%) |
| **Wind** |||
| `wind_speed_avg` | DOUBLE | Average wind speed (m/s) |
| `wind_dir_avg` | DOUBLE | Average wind direction (degrees) |
| `wind_max_hour` | DOUBLE | Maximum wind gust in hour (m/s) |
| `wind_min_hour` | DOUBLE | Minimum wind speed in hour (m/s) |
| **Pressure** |||
| `pressure_avg` | DOUBLE | Average atmospheric pressure (hPa) |
| `pressure_sea_avg` | DOUBLE | Average sea-level pressure (hPa) |
| **Solar Radiation** |||
| `radia_glob_avg` | DOUBLE | Average global radiation (W/m²) |
| `radia_glob_past1h_avg` | DOUBLE | Average 1-hour cumulative radiation |
| `sun_10min_sum` | DOUBLE | Sum of 10-minute sunshine duration |
| `sun_1h_sum` | DOUBLE | Sum of 1-hour sunshine duration |
| **Clouds & Visibility** |||
| `cloud_cover_avg` | DOUBLE | Average cloud cover (oktas, 0-8) |
| `visibility_avg` | DOUBLE | Average visibility (meters) |
| `visib_10min_avg` | DOUBLE | Average 10-minute visibility |
| **Precipitation** |||
| `precip_mm_hour_from10min` | DOUBLE | Total precipitation from 10-min observations (mm) |
| `precip_mm_hour_from1h` | DOUBLE | Maximum 1-hour precipitation reading (mm) |
| `precip_dur_10min_sum` | DOUBLE | Sum of 10-min precipitation duration |
| `precip_dur_1h` | DOUBLE | Maximum 1-hour precipitation duration |
| **Data Quality Metrics** |||
| `n_temp_obs` | BIGINT | Count of temperature observations in hour |
| `n_wind_obs` | BIGINT | Count of wind observations in hour |
| `n_radia_obs` | BIGINT | Count of radiation observations in hour |
| `n_precip10_obs` | BIGINT | Count of 10-min precipitation observations |

## Usage Examples

### Example 1: Basic Query with Station Names

```sql
SELECT
  w.station_id,
  s.station_name,
  s.dk_area,
  w.hour_utc,
  w.temp_dry_avg,
  w.wind_speed_avg,
  w.precip_mm_hour_from10min
FROM weather_station_hourly w
LEFT JOIN station_metadata s ON w.station_id = s.station_id
WHERE w.hour_utc >= '2025-01-01 00:00:00'
  AND s.dk_area = 'DK1'
LIMIT 100;
```

### Example 2: Join with Energy Production

```sql
-- Match weather and energy data by hour and DK area
WITH weather_by_area AS (
  SELECT
    s.dk_area,
    DATE(w.hour_utc) as date,
    HOUR(w.hour_utc) as hour,
    AVG(w.temp_dry_avg) as avg_temp,
    AVG(w.wind_speed_avg) as avg_wind_speed,
    AVG(w.radia_glob_avg) as avg_solar_radiation
  FROM weather_station_hourly w
  LEFT JOIN station_metadata s ON w.station_id = s.station_id
  WHERE w.hour_utc >= '2025-01-01'
  GROUP BY s.dk_area, DATE(w.hour_utc), HOUR(w.hour_utc)
),
energy_by_area AS (
  SELECT
    m.dk_area,
    DATE(e.HourDK) as date,
    HOUR(e.HourDK) as hour,
    SUM(e.SolarMWh) as total_solar_mwh,
    SUM(e.OnshoreWindMWh) as total_wind_mwh
  FROM energy_by_municipality e
  LEFT JOIN municipality_metadata m ON e.MunicipalityNo = m.municipality_code
  WHERE e.HourDK >= '2025-01-01'
  GROUP BY m.dk_area, DATE(e.HourDK), HOUR(e.HourDK)
)
SELECT
  w.dk_area,
  w.date,
  w.hour,
  w.avg_temp,
  w.avg_wind_speed,
  w.avg_solar_radiation,
  e.total_solar_mwh,
  e.total_wind_mwh
FROM weather_by_area w
JOIN energy_by_area e
  ON w.dk_area = e.dk_area
  AND w.date = e.date
  AND w.hour = e.hour
ORDER BY w.dk_area, w.date, w.hour
LIMIT 100;
```

### Example 3: Daily Weather Aggregates

```sql
-- Daily weather statistics per station
SELECT
  w.station_id,
  s.station_name,
  s.dk_area,
  DATE(w.hour_utc) as date,
  AVG(w.temp_dry_avg) as daily_avg_temp,
  MAX(w.temp_dry_avg) as daily_max_temp,
  MIN(w.temp_dry_avg) as daily_min_temp,
  AVG(w.wind_speed_avg) as daily_avg_wind,
  MAX(w.wind_max_hour) as daily_max_gust,
  SUM(w.precip_mm_hour_from10min) as daily_total_precip,
  AVG(w.radia_glob_avg) as daily_avg_radiation,
  COUNT(*) as hours_with_data
FROM weather_station_hourly w
LEFT JOIN station_metadata s ON w.station_id = s.station_id
WHERE DATE(w.hour_utc) >= '2025-01-01'
GROUP BY w.station_id, s.station_name, s.dk_area, DATE(w.hour_utc)
ORDER BY DATE(w.hour_utc), w.station_id
LIMIT 100;
```

### Example 4: Data Quality Check

```sql
-- Check data coverage per station
SELECT
  w.station_id,
  s.station_name,
  COUNT(*) as total_hours,
  AVG(w.n_temp_obs) as avg_temp_obs_per_hour,
  AVG(w.n_wind_obs) as avg_wind_obs_per_hour,
  AVG(w.n_radia_obs) as avg_radia_obs_per_hour,
  SUM(CASE WHEN w.temp_dry_avg IS NULL THEN 1 ELSE 0 END) as hours_missing_temp,
  SUM(CASE WHEN w.wind_speed_avg IS NULL THEN 1 ELSE 0 END) as hours_missing_wind
FROM weather_station_hourly w
LEFT JOIN station_metadata s ON w.station_id = s.station_id
WHERE DATE(w.hour_utc) >= '2025-01-01'
GROUP BY w.station_id, s.station_name
ORDER BY total_hours DESC
LIMIT 20;
```

## Benefits

1. **Much Smaller Dataset** - Hourly aggregates vs. millions of individual observations
2. **Easier to Join** - One row per station per hour, matches energy data granularity
3. **Pre-computed Features** - Averages, sums, mins, maxes already calculated
4. **Data Quality Metrics** - Observation counts help identify data gaps
5. **Faster Queries** - Aggregation done once, reused many times

## Query Performance Notes

- This table is stored as **Parquet** for efficient columnar access
- Consider partitioning by year/month if the dataset is very large
- The observation count columns (`n_*_obs`) are useful for filtering incomplete hours
- Join with `station_metadata` to get human-readable names and DK areas

## Next Steps

After the table is created, you can:

1. **Verify data**:
   ```sql
   SELECT COUNT(*) FROM weather_station_hourly;
   SELECT MIN(hour_utc), MAX(hour_utc) FROM weather_station_hourly;
   ```

2. **Check station coverage**:
   ```sql
   SELECT station_id, COUNT(*) as hours
   FROM weather_station_hourly
   GROUP BY station_id
   ORDER BY hours DESC;
   ```

3. **Join with energy data** for analysis and modeling
