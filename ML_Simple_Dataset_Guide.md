# Simple ML Dataset: Weather → Production & Consumption

## Overview

This dataset contains **weather features** to predict **total energy production** and **total energy consumption** for DK1 and DK2.

## Dataset Structure

### Input Features (12 features)

| Feature | Description | Unit | ML Importance |
|---------|-------------|------|---------------|
| `wind_speed_avg` | Average wind speed | m/s | High - drives wind production |
| `wind_speed_max` | Maximum wind speed | m/s | Medium - wind variability |
| `wind_direction_sin` | Wind direction (sine) | - | Low - directional component |
| `wind_direction_cos` | Wind direction (cosine) | - | Low - directional component |
| `solar_radiation_1h` | Solar radiation past hour | W/m² | High - drives solar production |
| `sunshine_duration_1h` | Sunshine duration | minutes | Medium - solar availability |
| `cloud_cover_avg` | Cloud cover | 0-8 oktas | Medium - blocks solar |
| `month_of_year` | Month (1-12) | - | High - seasonal patterns |
| `hour_of_day` | Hour (0-23) | - | High - daily patterns |
| `dk_area` | DK1 or DK2 | categorical | High - geographic split |
| `n_stations_wind` | Number of wind stations | count | Low - data quality |
| `n_stations_solar` | Number of solar stations | count | Low - data quality |

### Output Targets (2 targets)

| Target | Description | Unit | Typical Range (MWh) |
|--------|-------------|------|---------------------|
| `total_production_mwh` | Total energy produced | MWh | DK1: 900-4000, DK2: 300-1800 |
| `total_consumption_mwh` | Total energy consumed | MWh | DK1: 2500-6000, DK2: 1800-2600 |

## Data Availability

- **Date Range**: 2021-01-01 to 2025-11-30
- **Records**: ~86,000 hourly records per DK area
- **Total Rows**: ~172,000 (DK1 + DK2 combined)
- **Missing Data**: Minimal (production and consumption both required)

## Quick Start

### Get Sample Data (November 2025)

```sql
-- Use the query from ml_dataset_simple.sql
-- Just change the WHERE clause to filter desired date range

WHERE w.year = 2025 AND w.month = 11
  AND p.total_production_mwh IS NOT NULL
  AND c.total_consumption_mwh IS NOT NULL
```

### Export to CSV

```bash
# From your local machine
kubectl exec -n bd-bd-gr-08 hiveserver2-xxx -- \
  beeline -u jdbc:hive2://localhost:10000 \
  --outputformat=csv2 \
  -f /path/to/ml_dataset_simple.sql \
  > ml_training_data.csv
```

## ML Model Recommendations

### Model 1: Multi-Output Regression (Predict Both Targets)

**Approach**: Single model predicting both production and consumption

```python
from sklearn.ensemble import RandomForestRegressor
from sklearn.multioutput import MultiOutputRegressor

# Features
X = df[['wind_speed_avg', 'wind_speed_max', 'solar_radiation_1h',
        'sunshine_duration_1h', 'cloud_cover_avg', 'month_of_year',
        'hour_of_day']]  # + encode dk_area

# Targets
y = df[['total_production_mwh', 'total_consumption_mwh']]

# Model
model = MultiOutputRegressor(RandomForestRegressor(n_estimators=100))
model.fit(X_train, y_train)
```

### Model 2: Separate Models

**Production Model**:
- **Key Features**: wind_speed_avg, solar_radiation_1h, hour_of_day, month_of_year
- **Best Models**: Random Forest, XGBoost, Gradient Boosting

**Consumption Model**:
- **Key Features**: hour_of_day, month_of_year, dk_area (consumption less weather-dependent)
- **Best Models**: LSTM (for time series patterns), XGBoost

### Model 3: Separate by DK Area

Train separate models for DK1 and DK2 (different production/consumption scales).

## Data Preprocessing

### 1. Handle Categorical Variables

```python
# One-hot encode dk_area
df = pd.get_dummies(df, columns=['dk_area'], drop_first=True)
# Creates: dk_area_DK2 (1 if DK2, 0 if DK1)
```

### 2. Feature Scaling

```python
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
```

### 3. Train/Validation/Test Split

**Option A: Time-based split**
```python
train = df[df['year'] <= 2023]  # 2021-2023
val = df[df['year'] == 2024]    # 2024
test = df[df['year'] == 2025]   # 2025
```

**Option B: Random split**
```python
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
```

## Feature Engineering Ideas

### Temporal Features
```python
# Add more temporal patterns
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
df['is_night'] = ((df['hour_of_day'] >= 22) | (df['hour_of_day'] <= 6)).astype(int)
df['season'] = (df['month_of_year'] % 12 + 3) // 3  # 1=Winter, 2=Spring, etc.
```

### Lag Features
```python
# Previous hour's production/consumption
df['production_lag1h'] = df.groupby('dk_area')['total_production_mwh'].shift(1)
df['consumption_lag1h'] = df.groupby('dk_area')['total_consumption_mwh'].shift(1)

# Rolling averages
df['production_rolling_3h'] = df.groupby('dk_area')['total_production_mwh'].rolling(3).mean()
```

### Interaction Features
```python
# Wind-hour interaction (wind production peaks at certain hours)
df['wind_x_hour'] = df['wind_speed_avg'] * df['hour_of_day']

# Solar-cloud interaction
df['solar_x_cloudcover'] = df['solar_radiation_1h'] * (1 - df['cloud_cover_avg']/8)
```

## Complete Python Example

```python
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error

# 1. Load data
df = pd.read_csv('ml_training_data.csv')

# 2. Prepare features
# One-hot encode dk_area
df = pd.get_dummies(df, columns=['dk_area'], drop_first=True)

# Select features
features = [
    'wind_speed_avg', 'wind_speed_max',
    'wind_direction_sin', 'wind_direction_cos',
    'solar_radiation_1h', 'sunshine_duration_1h', 'cloud_cover_avg',
    'month_of_year', 'hour_of_day',
    'dk_area_DK2'  # 1 if DK2, 0 if DK1
]

X = df[features]
y = df[['total_production_mwh', 'total_consumption_mwh']]

# 3. Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# 4. Train model
model = MultiOutputRegressor(
    RandomForestRegressor(n_estimators=200, max_depth=20, random_state=42)
)
model.fit(X_train, y_train)

# 5. Evaluate
y_pred = model.predict(X_test)

print("Production R²:", r2_score(y_test['total_production_mwh'], y_pred[:, 0]))
print("Consumption R²:", r2_score(y_test['total_consumption_mwh'], y_pred[:, 1]))

print("Production MAE:", mean_absolute_error(y_test['total_production_mwh'], y_pred[:, 0]), "MWh")
print("Consumption MAE:", mean_absolute_error(y_test['total_consumption_mwh'], y_pred[:, 1]), "MWh")

# 6. Feature importance
importances = model.estimators_[0].feature_importances_  # For production
for feat, imp in sorted(zip(features, importances), key=lambda x: x[1], reverse=True):
    print(f"{feat}: {imp:.3f}")
```

## Expected Results

Based on the data patterns:

### Production Prediction
- **Expected R²**: 0.75-0.85 (weather strongly correlates with renewable production)
- **Key Features**: wind_speed_avg, solar_radiation_1h, hour_of_day, month_of_year

### Consumption Prediction
- **Expected R²**: 0.60-0.75 (consumption follows temporal patterns more than weather)
- **Key Features**: hour_of_day, month_of_year, is_weekend (if added)

## Files

- **Query**: `ml_dataset_simple.sql` - Complete SQL to generate dataset
- **Output**: 12 input features + 2 target variables
- **Size**: ~172,000 rows (86K per DK area)

## Next Steps

1. Run `ml_dataset_simple.sql` in Hive for your desired date range
2. Export to CSV or Parquet
3. Load into Python/R
4. Train baseline models (Random Forest, XGBoost)
5. Add feature engineering (lags, interactions)
6. Tune hyperparameters
7. Deploy for predictions!
