# Data Format Examples at Each Pipeline Stage

## 1. RAW DATA FORMAT (Long Format)

**Source Table**: `weather_observations_raw`
**Total Rows**: 198,866,490

**Format**: One row per observation (parameter measurement)

```
station_id | station_name | year | month | day | hour | minute | parameter_id | value | latitude | longitude
-----------|--------------|------|-------|-----|------|--------|--------------|-------|----------|----------
06019      | Silstrup     | 2025 | 11    | 1   | 12   | 0      | temp_dry     | 8.3   | 56.93    | 8.6412
06019      | Silstrup     | 2025 | 11    | 1   | 12   | 0      | temp_dew     | 7.1   | 56.93    | 8.6412
06019      | Silstrup     | 2025 | 11    | 1   | 12   | 0      | humidity     | 92.0  | 56.93    | 8.6412
06019      | Silstrup     | 2025 | 11    | 1   | 12   | 0      | wind_speed   | 4.5   | 56.93    | 8.6412
06019      | Silstrup     | 2025 | 11    | 1   | 12   | 0      | wind_dir     | 225.0 | 56.93    | 8.6412
06019      | Silstrup     | 2025 | 11    | 1   | 12   | 0      | pressure     | NULL  | 56.93    | 8.6412
```

**Characteristics**:
- **Vertical structure**: Each measurement is a separate row
- **6 rows** to represent one timestamp's data for one station
- Parameter name in `parameter_id` column, value in `value` column
- Not all parameters present for all stations/times (NULL or missing rows)

---

## 2. WIDE FORMAT (Pivoted Data)

**Source Table**: `weather_wide`
**Total Rows**: 16,760,614
**Conversion**: Pivot parameter_id → columns using `MAX(CASE WHEN...)`

**Format**: One row per station + timestamp

```
station_id | station_name | latitude | longitude | year | month | day | hour | minute | temp_dry | temp_dew | humidity | wind_speed | wind_dir | pressure
-----------|--------------|----------|-----------|------|-------|-----|------|--------|----------|----------|----------|------------|----------|----------
06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 12   | 0      | 8.3      | 7.1      | 92.0     | 4.5        | 225.0    | NULL
06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 13   | 0      | 8.5      | 7.3      | 91.5     | 4.8        | 230.0    | NULL
06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 14   | 0      | 8.7      | NULL     | 90.0     | 5.1        | 235.0    | 1013.2
```

**Characteristics**:
- **Horizontal structure**: All parameters as columns
- **1 row** represents one timestamp's complete data
- NULL values where parameter wasn't measured
- Easier to work with for ML models (feature columns)

**Conversion SQL**:
```sql
SELECT
  station_id,
  station_name,
  MAX(latitude) as latitude,
  MAX(longitude) as longitude,
  year, month, day, hour, minute,
  MAX(CASE WHEN parameter_id = 'temp_dry' THEN value END) as temp_dry,
  MAX(CASE WHEN parameter_id = 'temp_dew' THEN value END) as temp_dew,
  MAX(CASE WHEN parameter_id = 'humidity' THEN value END) as humidity,
  MAX(CASE WHEN parameter_id = 'wind_speed' THEN value END) as wind_speed,
  MAX(CASE WHEN parameter_id = 'wind_dir' THEN value END) as wind_dir,
  MAX(CASE WHEN parameter_id = 'pressure' THEN value END) as pressure
FROM weather_observations_raw
GROUP BY station_id, station_name, year, month, day, hour, minute;
```

---

## 3. FILTERED FORMAT (NULL Removal)

**Source**: Query on `weather_wide` with WHERE clause
**Rows**: 12,368,829 (73.7% of wide format retained)

**Filter**: Keep only records with core weather parameters

```
station_id | station_name | latitude | longitude | year | month | day | hour | minute | temp_dry | temp_dew | humidity | wind_speed | wind_dir | pressure
-----------|--------------|----------|-----------|------|-------|-----|------|--------|----------|----------|----------|------------|----------|----------
06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 12   | 0      | 8.3      | 7.1      | 92.0     | 4.5        | 225.0    | NULL
06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 13   | 0      | 8.5      | 7.3      | 91.5     | 4.8        | 230.0    | NULL
```

**Filter SQL**:
```sql
WHERE temp_dry IS NOT NULL
  AND humidity IS NOT NULL
  AND wind_speed IS NOT NULL
```

**Characteristics**:
- **Guaranteed core parameters**: temp_dry, humidity, wind_speed always present
- **Optional parameters**: temp_dew, wind_dir, pressure may still be NULL
- **Filtered out**: 4,391,785 rows (26.3%) missing one or more core parameters
- **Use case**: Ensures ML training data has minimum required features

---

## 4. JOINED FORMAT (Weather + Energy)

**Target Table**: `weather_energy_combined`
**Expected Rows**: 12,368,829 (all filtered weather records)
**Rows with Energy**: ~600K (for Oct-Nov 2025 only, once energy refetched)

**Format**: Weather data joined with energy production by municipality + timestamp

```
municipality_code | municipality_name | station_id | station_name | latitude | longitude | year | month | day | hour | minute | temp_dry | temp_dew | humidity | wind_speed | wind_dir | pressure | energy_hour_utc      | energy_hour_dk       | SolarMWh | OffshoreWindLt100MW_MWh | OffshoreWindGe100MW_MWh | OnshoreWindMWh | ThermalPowerMWh
------------------|-------------------|------------|--------------|----------|-----------|------|-------|-----|------|--------|----------|----------|----------|------------|----------|----------|----------------------|----------------------|----------|-------------------------|-------------------------|----------------|------------------
787               | Thisted           | 06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 12   | 0      | 8.3      | 7.1      | 92.0     | 4.5        | 225.0    | NULL     | 2025-11-01T12:00:00 | 2025-11-01T13:00:00 | 12.5     | 0.0                     | 0.0                     | 45.3           | 0.0
787               | Thisted           | 06019      | Silstrup     | 56.93    | 8.6412    | 2025 | 11    | 1   | 13   | 0      | 8.5      | 7.3      | 91.5     | 4.8        | 230.0    | NULL     | 2025-11-01T13:00:00 | 2025-11-01T14:00:00 | 10.8     | 0.0                     | 0.0                     | 48.7           | 0.0
787               | Thisted           | 06019      | Silstrup     | 56.93    | 8.6412    | 2021 | 5     | 15  | 10   | 0      | 15.2     | 10.5     | 72.0     | 3.2        | 180.0    | 1015.5   | NULL                 | NULL                 | NULL     | NULL                    | NULL                    | NULL           | NULL
```

**Join SQL**:
```sql
SELECT
  sm.municipality_code,
  sm.municipality_name,
  w.station_id,
  w.station_name,
  w.latitude,
  w.longitude,
  w.year, w.month, w.day, w.hour, w.minute,
  w.temp_dry,
  w.temp_dew,
  w.humidity,
  w.wind_speed,
  w.wind_dir,
  w.pressure,
  e.HourUTC as energy_hour_utc,
  e.HourDK as energy_hour_dk,
  e.SolarMWh,
  e.OffshoreWindLt100MW_MWh,
  e.OffshoreWindGe100MW_MWh,
  e.OnshoreWindMWh,
  e.ThermalPowerMWh
FROM weather_wide w
INNER JOIN station_municipality_mapping sm
  ON w.station_id = sm.station_id
LEFT JOIN energy_production e
  ON CAST(sm.municipality_code AS STRING) = e.MunicipalityNo
  AND w.year = e.year
  AND w.month = e.month
  AND w.day = e.day
  AND w.hour = e.hour
WHERE w.temp_dry IS NOT NULL
  AND w.humidity IS NOT NULL
  AND w.wind_speed IS NOT NULL;
```

**Characteristics**:
- **All weather records preserved** (LEFT JOIN keeps all weather even without energy match)
- **Energy columns NULL** for dates outside energy data range
- **Energy columns populated** for dates with matching energy data
- **One row per station** (some municipalities have multiple stations)
- **Municipality-level energy** joined with station-level weather

**Mapping**:
- Station "Silstrup" (06019) → Municipality "Thisted" (787)
- 61 total weather stations mapped to municipalities
- Some municipalities have 2-3 stations (values will need averaging later)

---

## Summary: Data Transformation Pipeline

```
RAW (Long Format)
198,866,490 rows × many parameters
↓ PIVOT (GROUP BY + CASE WHEN)
WIDE Format
16,760,614 rows × parameter columns
↓ FILTER (WHERE core params NOT NULL)
FILTERED Wide
12,368,829 rows × parameter columns
↓ JOIN (station→municipality, timestamp match)
JOINED (Weather + Energy)
12,368,829 rows × (weather + energy columns)
```

**Data Quality at Each Stage**:
1. **Raw**: Complete but hard to use (vertical)
2. **Wide**: Easy to use but has NULLs
3. **Filtered**: Quality data, ready for ML
4. **Joined**: Complete dataset for weather-energy correlation analysis
