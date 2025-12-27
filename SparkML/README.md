# SparkML Energy Prediction Models

This directory contains machine learning models for predicting energy production and consumption in Denmark based on weather features.

## Overview

### Models
- **Production Model**: Predicts total energy production (MWh) from weather conditions
- **Consumption Model**: Predicts total energy consumption (MWh) from temporal patterns

### Features (Inputs)

#### Weather Features (9)
- `wind_speed_avg` - Average wind speed (m/s)
- `wind_speed_max` - Maximum wind speed (m/s)
- `wind_direction_sin` - Wind direction sine component
- `wind_direction_cos` - Wind direction cosine component
- `solar_radiation_1h` - Solar radiation past hour (W/m²)
- `sunshine_duration_1h` - Sunshine duration past hour
- `cloud_cover_avg` - Cloud cover (0-8 oktas)
- `n_stations_wind` - Number of wind stations used
- `n_stations_solar` - Number of solar stations used

#### Temporal Features (2)
- `month_of_year` - Month (1-12)
- `hour_of_day` - Hour (0-23)

#### Categorical Features (1)
- `dk_area` - DK1 or DK2 (one-hot encoded)

**Total: 12 features**

### Targets (Outputs)
- `total_production_mwh` - Total energy production (MWh)
- `total_consumption_mwh` - Total energy consumption (MWh)

## Data Pipeline

### 1. Create ML Training Table in Hive

First, create the ML training dataset table from the weather, production, and consumption data:

```bash
kubectl apply -f k8s/create-ml-training-table.yaml
kubectl logs -n bd-bd-gr-08 -f job/create-ml-training-table
```

This creates a `ml_training_data` table with:
- Date range: 2021-2025
- ~86,000 records per DK area
- All features and targets pre-joined

### 2. Verify Table

```bash
kubectl exec -n bd-bd-gr-08 hiveserver2-xxx -- beeline -u jdbc:hive2://localhost:10000 -e "
SELECT COUNT(*) FROM ml_training_data;
SELECT * FROM ml_training_data LIMIT 5;
"
```

## Training Models

### Option 1: Train Locally (Recommended for Development)

If you have the data exported or want to test:

```bash
# Install dependencies
pip install pyspark==3.5.0 pandas

# Run training (will use local Spark)
python3 SparkML/train_energy_model.py
```

**Note**: For local training, you'll need to either:
1. Export the Hive table to CSV/Parquet and load it locally
2. Configure Spark to connect to the remote Hive metastore

### Option 2: Train on Kubernetes (Production)

Create a Spark job that runs on Kubernetes:

```yaml
# k8s/spark-ml-training-job.yaml (to be created)
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-ml-training
  namespace: bd-bd-gr-08
spec:
  template:
    spec:
      containers:
      - name: spark-training
        image: bitnami/spark:3.5.0
        command: ["/bin/bash", "-c"]
        args:
          - |
            pip install --no-cache-dir pyspark==3.5.0
            python3 /app/SparkML/train_energy_model.py
        volumeMounts:
        - name: code
          mountPath: /app
      volumes:
      - name: code
        persistentVolumeClaim:
          claimName: weather-data-pvc
      restartPolicy: Never
```

## Configuration

Edit `train_energy_model.py` to configure:

```python
CONFIG = {
    # Data source
    "USE_HIVE_TABLE": True,  # Load from Hive table
    "ML_TABLE_NAME": "ml_training_data",

    # Date ranges
    "TRAIN_START_YEAR": 2021,  # Training start
    "TRAIN_END_YEAR": 2024,    # Training end
    "VAL_YEAR": 2025,          # Validation year
    "VAL_MONTHS": [1,2,3,4,5,6,7,8,9,10],  # Jan-Oct
    "TEST_YEAR": 2025,         # Test year
    "TEST_MONTH": 11,          # November

    # Model selection
    "TRAIN_PRODUCTION": True,   # Train production model
    "TRAIN_CONSUMPTION": True,  # Train consumption model

    # Hyperparameters
    "RF": {
        "numTrees": [50, 100],
        "maxDepth": [6, 10],
        "minInstancesPerNode": [1, 5]
    }
}
```

## Data Splits

The training uses **time-based splits** to prevent data leakage:

- **Training**: 2021-2024 (4 years, ~70,000 records per area)
- **Validation**: Jan-Oct 2025 (~14,000 records per area)
- **Test**: Nov 2025 (~1,400 records per area)

This ensures the model is tested on truly future data.

## Model Pipeline

Each model uses the following Spark ML pipeline:

1. **StringIndexer** - Convert dk_area (DK1/DK2) to numeric indices
2. **OneHotEncoder** - One-hot encode dk_area
3. **Imputer** - Fill missing values with mean
4. **VectorAssembler** - Combine all features into feature vector
5. **StandardScaler** - Normalize features (zero mean, unit variance)
6. **RandomForestRegressor** - Train Random Forest model

## Hyperparameter Tuning

Uses **TrainValidationSplit** with grid search:

- **numTrees**: [50, 100] - Number of trees in forest
- **maxDepth**: [6, 10] - Maximum tree depth
- **minInstancesPerNode**: [1, 5] - Minimum instances per leaf

Best model selected by RMSE on validation set.

## Output

Trained models are saved to `SparkML/models/` with naming:

```
energy_prediction_model_{target}_{model_type}_{timestamp}/
├── data/                    # Model binary data
├── metadata/                # Pipeline metadata
└── metadata.json            # Training metrics and config
```

Example:
```
models/energy_prediction_model_production_RF_20251227_210530/
```

### Metadata JSON

```json
{
  "target": "total_production_mwh",
  "model_type": "RandomForest",
  "features": ["wind_speed_avg", "wind_speed_max", ...],
  "categorical_features": ["dk_area"],
  "validation_metrics": {
    "val_rmse": 245.32,
    "val_mae": 189.45,
    "val_r2": 0.8234
  },
  "test_metrics": {
    "test_rmse": 267.89,
    "test_mae": 201.56,
    "test_r2": 0.8012
  },
  "config": {...},
  "created_at": "2025-12-27T21:05:30"
}
```

## Loading Trained Models

```python
from pyspark.ml import PipelineModel

# Load model
model_path = "SparkML/models/energy_prediction_model_production_RF_20251227_210530"
model = PipelineModel.load(model_path)

# Make predictions
predictions = model.transform(test_data)
predictions.select("prediction", "total_production_mwh").show()
```

## Expected Performance

Based on the dataset characteristics:

### Production Model
- **RMSE**: ~200-300 MWh (varies by weather conditions)
- **R²**: 0.75-0.85 (weather explains 75-85% of variance)
- Production is highly correlated with wind speed and solar radiation

### Consumption Model
- **RMSE**: ~100-200 MWh
- **R²**: 0.80-0.90 (temporal patterns are very predictable)
- Consumption follows strong hourly and daily patterns

## Next Steps

1. **Feature Engineering**
   - Add lag features (previous hour's production/consumption)
   - Add rolling averages (3-hour, 24-hour windows)
   - Add interaction terms (wind_speed × hour_of_day)

2. **Model Improvements**
   - Try GBT (Gradient Boosted Trees)
   - Ensemble multiple models
   - Separate models for DK1 vs DK2

3. **Real-time Inference**
   - Deploy model as REST API
   - Integrate with real-time weather stream
   - Generate hourly forecasts

## Files

- `train_energy_model.py` - Main training script (NEW - uses weather features)
- `train_model.py` - Old training script (price prediction with tokens)
- `models/` - Saved models directory
- `README.md` - This file

## Troubleshooting

### "Table not found: ml_training_data"

Create the table first:
```bash
kubectl apply -f k8s/create-ml-training-table.yaml
```

### "No records in train/val/test split"

Check date ranges in CONFIG match your data:
```python
CONFIG["TRAIN_START_YEAR"] = 2021  # Adjust based on your data
```

### Spark Connect errors

Make sure you're using PySpark 3.5.x (not 4.x):
```bash
pip uninstall pyspark
pip install 'pyspark==3.5.0'
```

## References

- ML Dataset Query: [`../ml_dataset_simple.sql`](../ml_dataset_simple.sql)
- ML Dataset Guide: [`../ML_Simple_Dataset_Guide.md`](../ML_Simple_Dataset_Guide.md)
- Hive Tables: [`../Hive_Tables_Documentation.md`](../Hive_Tables_Documentation.md)
