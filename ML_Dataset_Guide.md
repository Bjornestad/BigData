# Machine Learning Training Dataset Guide

## Overview

This guide explains how to create a complete ML training dataset with **weather features as inputs** to predict **energy production and consumption as outputs**.

## Dataset Structure

### Input Features (Weather Data)
- **Wind features**: Speed (avg, max), Direction (sin/cos components)
- **Solar features**: Solar radiation, Sunshine duration, Cloud cover
- **Temporal features**: Year, Month, Day, Hour, Day of week, Weekend indicator
- **Geographic**: DK area (DK1 or DK2)

### Output/Target Variables
- **Solar production** (MWh)
- **Wind production** (Onshore + Offshore, MWh)
- **Thermal production** (MWh)
- **Total production** (MWh)
- **Total consumption** (MWh)
- **Net energy balance** (Production - Consumption)
- **Self-sufficiency** (Production / Consumption %)

## Data Sources

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `weather_wind_solar_area_hourly` | Weather features by DK area | wind_speed, solar_radiation, cloud_cover |
| `energy_production` + `municipality_metadata` | Production by DK area | SolarMWh, OnshoreWindMWh, etc. |
| `consumption_coverage_location` | Consumption by DK area | PriceArea, ShareMWh |

## Date Range
- **Available**: 2021-01-01 to 2025-11-30
- **Records**: ~86,000 hourly observations per DK area
- **Total**: ~172,000 rows (DK1 + DK2)

## Quick Start Queries

### 1. Simple Weather + Production Dataset

```sql
SELECT
  w.year, w.month, w.day, w.hour, w.dk_area,

  -- Weather features (INPUT)
  w.wind_speed_mean_area as wind_speed,
  w.wind_speed_max_area as wind_speed_max,
  w.radia_glob_past1h_area as solar_radiation,
  w.cloud_cover_mean_area as cloud_cover,

  -- Production targets (OUTPUT)
  p.solar_mwh,
  p.wind_mwh,
  p.total_mwh

FROM weather_wind_solar_area_hourly w
JOIN (
  SELECT
    p.year, p.month, p.day, p.hour, m.dk_area,
    ROUND(SUM(COALESCE(p.SolarMWh, 0)), 2) as solar_mwh,
    ROUND(SUM(COALESCE(p.OnshoreWindMWh,0) + COALESCE(p.OffshoreWindLt100MW_MWh,0) + COALESCE(p.OffshoreWindGe100MW_MWh,0)), 2) as wind_mwh,
    ROUND(SUM(COALESCE(p.SolarMWh,0) + COALESCE(p.OnshoreWindMWh,0) + COALESCE(p.OffshoreWindLt100MW_MWh,0) + COALESCE(p.OffshoreWindGe100MW_MWh,0) + COALESCE(p.ThermalPowerMWh,0)), 2) as total_mwh
  FROM energy_production p
  JOIN municipality_metadata m ON p.MunicipalityNo = m.municipality_code
  WHERE p.year = 2025 AND p.month = 11  -- Adjust date range
  GROUP BY p.year, p.month, p.day, p.hour, m.dk_area
) p ON w.dk_area = p.dk_area AND w.year = p.year AND w.month = p.month AND w.day = p.day AND w.hour = p.hour

WHERE w.year = 2025 AND w.month = 11
ORDER BY w.year, w.month, w.day, w.hour, w.dk_area;
```

### 2. Weather + Production + Consumption Dataset

Run the production query above and the consumption query separately, then join them in your ML framework (Python/R).

**Production Query**: Use query above

**Consumption Query**:
```sql
SELECT
  year, month, day, hour,
  PriceArea as dk_area,
  ROUND(SUM(ShareMWh), 2) as consumption_mwh
FROM consumption_coverage_location
WHERE year = 2025 AND month = 11  -- Match production date range
GROUP BY year, month, day, hour, PriceArea
ORDER BY year, month, day, hour, dk_area;
```

## Export for ML Framework

### Option 1: Export to CSV (via beeline)

```bash
kubectl exec -n bd-bd-gr-08 hiveserver2-xxx -- beeline -u jdbc:hive2://localhost:10000 \
  --outputformat=csv2 \
  -f /path/to/your/query.sql \
  > ml_training_data.csv
```

### Option 2: Save as Hive Table

```sql
CREATE TABLE ml_training_data STORED AS PARQUET AS
SELECT
  w.year, w.month, w.day, w.hour, w.dk_area,
  w.wind_speed_mean_area,
  w.radia_glob_past1h_area,
  -- ... add all features
  p.total_production_mwh,
  c.total_consumption_mwh
FROM weather_wind_solar_area_hourly w
JOIN production_cte p ON ...
JOIN consumption_cte c ON ...
WHERE w.year >= 2021;
```

Then export:
```bash
hdfs dfs -get /user/hive/warehouse/ml_training_data/*.parquet ./
```

## ML Model Suggestions

### 1. **Wind Energy Production Prediction**
**Input Features**:
- wind_speed_mean_area
- wind_speed_max_area
- wind_dir_sin_area, wind_dir_cos_area
- hour_of_day, month_of_year

**Target**: total_wind_production_mwh

**Model**: Random Forest, Gradient Boosting, Neural Network

### 2. **Solar Energy Production Prediction**
**Input Features**:
- solar_radiation_1h (radia_glob_past1h_area)
- sunshine_duration_1h
- cloud_cover_mean_area
- hour_of_day, month_of_year

**Target**: solar_production_mwh

**Model**: Random Forest, XGBoost

### 3. **Total Consumption Prediction**
**Input Features**:
- hour_of_day, day_of_week, is_weekend
- month_of_year
- (Optional) temperature if available

**Target**: total_consumption_mwh

**Model**: LSTM/RNN (for time series), XGBoost

### 4. **Multi-Output Regression**
Predict both production AND consumption simultaneously from weather features.

## Data Preprocessing Tips

1. **Handle Missing Values**: Check for NULLs in weather data
2. **Feature Engineering**:
   - Create lag features (previous hour's production/consumption)
   - Rolling averages (3-hour, 24-hour windows)
   - Interaction terms (wind_speed * hour_of_day)
3. **Normalization**: Scale wind speed, solar radiation to [0,1] or standardize
4. **Train/Val/Test Split**:
   - Training: 2021-2024
   - Validation: Jan-Oct 2025
   - Test: Nov 2025

## Sample Python Workflow

```python
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

# Load data
df = pd.read_csv('ml_training_data.csv')

# Features and target
features = ['wind_speed_mean_area', 'radia_glob_past1h_area',
            'cloud_cover_mean_area', 'hour', 'month']
X = df[features]
y = df['total_production_mwh']

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate
score = model.score(X_test, y_test)
print(f"R² Score: {score:.3f}")
```

## Complete Query File

The full ML dataset query is available in:
- **File**: `ml_training_dataset.sql`
- **Contains**: All features, temporal encodings, and targets
- **Usage**: Adjust date range in WHERE clauses, then run in Hive

## Column Reference

### Weather Features
- `wind_speed_mean_area`: Average wind speed (m/s)
- `wind_speed_max_area`: Maximum wind speed (m/s)
- `wind_dir_sin_area`: Wind direction sine component
- `wind_dir_cos_area`: Wind direction cosine component
- `radia_glob_past1h_area`: Solar radiation past hour (W/m²)
- `sun_last1h_glob_area`: Sunshine duration past hour
- `cloud_cover_mean_area`: Cloud cover (0-8 oktas)

### Production Targets
- `solar_production_mwh`: Solar energy produced
- `onshore_wind_production_mwh`: Onshore wind energy
- `offshore_wind_production_mwh`: Offshore wind energy
- `total_wind_production_mwh`: All wind sources
- `thermal_production_mwh`: Thermal/fossil fuel
- `total_production_mwh`: All energy sources

### Consumption Target
- `total_consumption_mwh`: Total energy consumed in DK area
