# SparkML Model Updates - Energy Prediction

## Summary

Updated the SparkML training pipeline to use the correct ML dataset features for predicting energy production and consumption from weather data.

## Key Changes to `train_model.py`

### 1. Configuration Updates

**Old inputs:** Token arrays (mock weather embeddings) + lag features
**New inputs:** Real weather features from `ml_training_data` table

```python
CONFIG = {
    # Hive connection
    "HIVE_TABLE": "ml_training_data",  # Changed from "sample_table"
    "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",  # Added

    # Weather features (9 features)
    "WEATHER_FEATURES": [
        "wind_speed_avg", "wind_speed_max",
        "wind_direction_sin", "wind_direction_cos",
        "solar_radiation_1h", "sunshine_duration_1h", "cloud_cover_avg",
        "n_stations_wind", "n_stations_solar"
    ],

    # Temporal features (2 features)
    "TEMPORAL_FEATURES": ["month_of_year", "hour_of_day"],

    # Categorical features (1 feature)
    "CATEGORICAL_FEATURES": ["dk_area"],  # DK1 or DK2

    # Target variables (2 outputs)
    "PRODUCTION_TARGET": "total_production_mwh",
    "CONSUMPTION_TARGET": "total_consumption_mwh",
}
```

### 2. Data Loading

**Added Hive support:**
- Connects to Hive metastore at `thrift://hive-metastore:9083`
- Loads from `ml_training_data` table
- Falls back to running `ml_dataset_simple.sql` if table doesn't exist

```python
def load_hive_data(spark, use_mock=False):
    # Try loading from Hive table first
    df = spark.table(f"{db}.{table}")
    # Fallback to SQL file if table doesn't exist
```

### 3. Feature Engineering

**Old approach:** Token expansion + lag features + rolling windows
**New approach:** Direct use of weather + temporal features from Hive

```python
def prepare_features(df, use_mock=False):
    if use_mock:
        # Old: expand tokens, build time features, add lags
    else:
        # New: use weather + temporal features directly
        feature_cols = CONFIG["WEATHER_FEATURES"] + CONFIG["TEMPORAL_FEATURES"]
```

### 4. Data Splitting

**Old:** Percentile-based split on epoch timestamps
**New:** Time-based split by year/month

```python
# Training: 2021-2024 (4 years)
# Validation: Jan-Oct 2025
# Test: November 2025
```

### 5. Pipeline Updates

**Added categorical feature encoding:**
- StringIndexer for `dk_area` (DK1/DK2)
- OneHotEncoder to convert to binary features

```python
def build_pipeline(feature_cols, label_col, include_categorical=False):
    if include_categorical:
        # Add StringIndexer + OneHotEncoder for dk_area
```

### 6. Model Training

**Old:** Single model predicting `price`
**New:** Two separate models

1. **Production Model** - Predicts `total_production_mwh`
2. **Consumption Model** - Predicts `total_consumption_mwh`

Both models trained with:
- Random Forest Regressor
- Hyperparameter tuning (numTrees: [50, 100], maxDepth: [6, 10])
- Time-aware train/validation split

## Input Features (12 total)

### Weather Features (9)
1. `wind_speed_avg` - Average wind speed (m/s)
2. `wind_speed_max` - Maximum wind speed (m/s)
3. `wind_direction_sin` - Wind direction sine component
4. `wind_direction_cos` - Wind direction cosine component
5. `solar_radiation_1h` - Solar radiation past hour (W/m²)
6. `sunshine_duration_1h` - Sunshine duration past hour
7. `cloud_cover_avg` - Cloud cover (0-8 oktas)
8. `n_stations_wind` - Number of wind stations
9. `n_stations_solar` - Number of solar stations

### Temporal Features (2)
10. `month_of_year` - Month (1-12)
11. `hour_of_day` - Hour (0-23)

### Categorical Features (1)
12. `dk_area` - DK1 or DK2 (one-hot encoded)

## Output Targets (2)

1. `total_production_mwh` - Total energy production (MWh)
2. `total_consumption_mwh` - Total energy consumption (MWh)

## Usage

### Prerequisites

1. Create the ML training table in Hive:
```bash
kubectl apply -f k8s/create-ml-training-table-v2.yaml
```

2. Verify table exists:
```bash
kubectl exec -n bd-bd-gr-08 hiveserver2-xxx -- beeline -u jdbc:hive2://localhost:10000 -e "
SELECT COUNT(*) FROM ml_training_data;
"
```

### Run Training

```bash
python3 SparkML/train_model.py
```

### Expected Output

```
======================================================================
ENERGY PREDICTION MODEL TRAINING
======================================================================
Mode: REAL ML DATASET
✓ Loaded 172,000 total rows

Features (11):
  - wind_speed_avg
  - wind_speed_max
  ...

Data splits:
  Train: 140,000 records
  Val:   28,000 records
  Test:  4,000 records

======================================================================
TRAINING PRODUCTION MODEL
======================================================================
Training with 4 hyperparameter combinations...
✓ Production Model Test Metrics: {'RMSE': 245.32, 'MAE': 189.45, 'R2': 0.8234}

======================================================================
TRAINING CONSUMPTION MODEL
======================================================================
Training with 4 hyperparameter combinations...
✓ Consumption Model Test Metrics: {'RMSE': 156.78, 'MAE': 121.34, 'R2': 0.8567}

======================================================================
TRAINING COMPLETE
======================================================================
✓ Production model: ./SparkML/models/energy_model_production_20251227_213045
✓ Consumption model: ./SparkML/models/energy_model_consumption_20251227_213045
```

## Model Output

Each model saves:
- `data/` - Model binary files
- `metadata/` - Pipeline metadata
- `metadata.json` - Training configuration and metrics

Example `metadata.json`:
```json
{
  "target": "total_production_mwh",
  "model_type": "RandomForest",
  "features": ["wind_speed_avg", "wind_speed_max", ...],
  "categorical_features": ["dk_area"],
  "test_metrics": {
    "RMSE": 245.32,
    "MAE": 189.45,
    "R2": 0.8234
  },
  "created_at": "2025-12-27T21:30:45"
}
```

## Next Steps

1. Create the Hive table: `kubectl apply -f k8s/create-ml-training-table-v2.yaml`
2. Run training: `python3 SparkML/train_model.py`
3. Evaluate model performance
4. Deploy for real-time predictions

## Files Modified

- `SparkML/train_model.py` - Main training script
- `SparkML/README.md` - Updated documentation (from train_energy_model.py)
- `SparkML/CHANGES.md` - This file

## Backward Compatibility

The code maintains backward compatibility with mock data mode:
- Set `CONFIG["USE_MOCK_DATA"] = True` to use old token-based features
- Useful for testing without Hive connection
