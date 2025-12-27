"""
SparkML Training Pipeline for Energy Production & Consumption Prediction

This script trains ML models to predict:
- Total energy production (MWh)
- Total energy consumption (MWh)

Using weather features as inputs from the ML dataset.
"""

import os
import json
import datetime
from typing import List, Dict, Tuple

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, Imputer, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

# ============================================
# CONFIGURATION
# ============================================

CONFIG = {
    # Hive connection
    "HIVE_DB": "default",
    "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",

    # Data source - either use Hive table or query directly
    "USE_HIVE_TABLE": True,  # Set to True to load from pre-created ml_training_data table
    "ML_TABLE_NAME": "ml_training_data",  # If table created from ml_dataset_simple.sql

    # Or use direct SQL query
    "USE_DIRECT_QUERY": False,  # Set to True to run ml_dataset_simple.sql directly
    "ML_QUERY_FILE": "../ml_dataset_simple.sql",

    # Date range for training
    "TRAIN_START_YEAR": 2021,
    "TRAIN_END_YEAR": 2024,
    "VAL_YEAR": 2025,
    "VAL_MONTHS": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],  # Jan-Oct 2025
    "TEST_YEAR": 2025,
    "TEST_MONTH": 11,  # November 2025

    # Model output
    "MODEL_OUTPUT_PATH": "./SparkML/models",
    "MODEL_NAME": "energy_prediction_model",

    # Training parameters
    "TRAIN_PRODUCTION": True,   # Train production prediction model
    "TRAIN_CONSUMPTION": True,  # Train consumption prediction model

    # Model hyperparameters (search space)
    "RF": {
        "numTrees": [50, 100],
        "maxDepth": [6, 10],
        "minInstancesPerNode": [1, 5]
    },
    "GBT": {
        "maxIter": [50, 100],
        "maxDepth": [4, 8],
        "stepSize": [0.1, 0.01]
    },

    # Spark settings
    "SPARK_MASTER": "local[*]",
    "APP_NAME": "EnergyPredictionML",
    "SHUFFLE_PARTITIONS": 8,
}

# Weather features (inputs)
WEATHER_FEATURES = [
    "wind_speed_avg",
    "wind_speed_max",
    "wind_direction_sin",
    "wind_direction_cos",
    "solar_radiation_1h",
    "sunshine_duration_1h",
    "cloud_cover_avg",
    "n_stations_wind",
    "n_stations_solar"
]

# Temporal features (inputs)
TEMPORAL_FEATURES = [
    "month_of_year",
    "hour_of_day"
]

# Categorical features
CATEGORICAL_FEATURES = ["dk_area"]

# Target variables (outputs)
PRODUCTION_TARGET = "total_production_mwh"
CONSUMPTION_TARGET = "total_consumption_mwh"


# ============================================
# SPARK SESSION
# ============================================

def create_spark_session() -> SparkSession:
    """Create Spark session with Hive support"""

    # Clean up any existing sessions
    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark is not None:
            print("Stopping existing Spark session...")
            existing_spark.stop()
    except:
        pass

    # Remove Spark Connect environment variables
    for env_var in ["SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED"]:
        if env_var in os.environ:
            del os.environ[env_var]

    # Create session with Hive support
    builder = SparkSession.builder \
        .appName(CONFIG["APP_NAME"]) \
        .config("spark.sql.shuffle.partitions", CONFIG["SHUFFLE_PARTITIONS"]) \
        .config("spark.sql.connect.enabled", "false") \
        .master(CONFIG["SPARK_MASTER"])

    # Add Hive metastore if configured
    if CONFIG.get("HIVE_METASTORE_URI"):
        builder = builder.config("hive.metastore.uris", CONFIG["HIVE_METASTORE_URI"])
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()

    # Verify JVM mode
    if not hasattr(spark, 'sparkContext'):
        raise RuntimeError(
            "ERROR: Spark Connect mode detected. MLlib requires JVM mode.\n"
            "Please use PySpark 3.5.x or set SPARK_CONNECT_MODE_ENABLED=false"
        )

    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark Session created (JVM mode)")
    print(f"  Spark version: {spark.version}")

    return spark


# ============================================
# DATA LOADING
# ============================================

def load_ml_dataset(spark: SparkSession) -> Tuple[object, object, object]:
    """
    Load ML dataset and split into train/validation/test sets.

    Returns:
        train_df, val_df, test_df
    """

    if CONFIG["USE_HIVE_TABLE"]:
        # Load from pre-created Hive table
        table_name = f"{CONFIG['HIVE_DB']}.{CONFIG['ML_TABLE_NAME']}"
        print(f"Loading data from Hive table: {table_name}")
        df = spark.table(table_name)

    elif CONFIG["USE_DIRECT_QUERY"]:
        # Run the SQL query directly
        query_file = CONFIG["ML_QUERY_FILE"]
        print(f"Loading data from SQL query: {query_file}")

        with open(query_file, 'r') as f:
            query = f.read()

        df = spark.sql(query)
    else:
        raise ValueError("Must set either USE_HIVE_TABLE or USE_DIRECT_QUERY to True")

    print(f"✓ Loaded {df.count():,} total records")

    # Time-based split
    print("\nSplitting data by time...")

    # Training: 2021-2024
    train_df = df.filter(
        (F.col("year") >= CONFIG["TRAIN_START_YEAR"]) &
        (F.col("year") <= CONFIG["TRAIN_END_YEAR"])
    )

    # Validation: Jan-Oct 2025
    val_df = df.filter(
        (F.col("year") == CONFIG["VAL_YEAR"]) &
        (F.col("month").isin(CONFIG["VAL_MONTHS"]))
    )

    # Test: Nov 2025
    test_df = df.filter(
        (F.col("year") == CONFIG["TEST_YEAR"]) &
        (F.col("month") == CONFIG["TEST_MONTH"])
    )

    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()

    print(f"  Train: {train_count:,} records ({CONFIG['TRAIN_START_YEAR']}-{CONFIG['TRAIN_END_YEAR']})")
    print(f"  Val:   {val_count:,} records ({CONFIG['VAL_YEAR']} Jan-Oct)")
    print(f"  Test:  {test_count:,} records ({CONFIG['TEST_YEAR']} Nov)")

    return train_df, val_df, test_df


# ============================================
# FEATURE ENGINEERING
# ============================================

def build_feature_pipeline(feature_cols: List[str], label_col: str,
                           include_categorical: bool = True) -> Tuple[Pipeline, object]:
    """
    Build Spark ML pipeline for feature processing and model training.

    Pipeline stages:
    1. StringIndexer + OneHotEncoder for dk_area (if include_categorical)
    2. Imputer for missing values
    3. VectorAssembler to combine all features
    4. StandardScaler for normalization
    5. RandomForestRegressor

    Args:
        feature_cols: List of numeric feature column names
        label_col: Target variable name
        include_categorical: Whether to include dk_area encoding

    Returns:
        (pipeline, estimator)
    """

    stages = []
    assembler_cols = feature_cols.copy()

    # Handle categorical feature (dk_area)
    if include_categorical and "dk_area" in CATEGORICAL_FEATURES:
        indexer = StringIndexer(
            inputCol="dk_area",
            outputCol="dk_area_indexed",
            handleInvalid="keep"
        )
        encoder = OneHotEncoder(
            inputCols=["dk_area_indexed"],
            outputCols=["dk_area_encoded"]
        )
        stages.extend([indexer, encoder])
        assembler_cols.append("dk_area_encoded")

    # Impute missing values
    imputer = Imputer(
        inputCols=feature_cols,
        outputCols=feature_cols,
        strategy="mean"
    )
    stages.append(imputer)

    # Assemble all features into vector
    assembler = VectorAssembler(
        inputCols=assembler_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    stages.append(assembler)

    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    stages.append(scaler)

    # Random Forest estimator
    estimator = RandomForestRegressor(
        featuresCol="features",
        labelCol=label_col,
        predictionCol="prediction",
        seed=42
    )
    stages.append(estimator)

    pipeline = Pipeline(stages=stages)

    return pipeline, estimator


# ============================================
# TRAINING & EVALUATION
# ============================================

def train_model_with_tuning(pipeline: Pipeline, estimator: object,
                            train_df: object, val_df: object,
                            param_grid: List, label_col: str) -> Tuple[object, Dict]:
    """
    Train model with hyperparameter tuning using TrainValidationSplit.

    Returns:
        (best_model, metrics)
    """

    evaluator = RegressionEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="rmse"
    )

    # Combine train + val for TrainValidationSplit
    union_df = train_df.unionByName(val_df)
    train_size = train_df.count()
    total_size = union_df.count()
    train_ratio = train_size / total_size

    print(f"\n  Training with {len(param_grid)} hyperparameter combinations")
    print(f"  Train ratio: {train_ratio:.3f}")

    tvs = TrainValidationSplit(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        trainRatio=train_ratio,
        parallelism=2,
        seed=42
    )

    print("  Fitting model...")
    tvs_model = tvs.fit(union_df)
    best_model = tvs_model.bestModel

    # Get validation metrics
    val_predictions = best_model.transform(val_df)
    val_rmse = evaluator.evaluate(val_predictions)
    val_mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction",
                                   metricName="mae").evaluate(val_predictions)
    val_r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction",
                                 metricName="r2").evaluate(val_predictions)

    metrics = {
        "val_rmse": val_rmse,
        "val_mae": val_mae,
        "val_r2": val_r2
    }

    print(f"  ✓ Validation RMSE: {val_rmse:.2f}")
    print(f"    Validation MAE:  {val_mae:.2f}")
    print(f"    Validation R²:   {val_r2:.4f}")

    return best_model, metrics


def evaluate_on_test(model: object, test_df: object, label_col: str) -> Dict:
    """Evaluate model on test set"""

    print(f"\n  Evaluating on test set...")
    predictions = model.transform(test_df)

    rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction",
                               metricName="rmse").evaluate(predictions)
    mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction",
                              metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction",
                             metricName="r2").evaluate(predictions)

    metrics = {
        "test_rmse": rmse,
        "test_mae": mae,
        "test_r2": r2
    }

    print(f"  ✓ Test RMSE: {rmse:.2f}")
    print(f"    Test MAE:  {mae:.2f}")
    print(f"    Test R²:   {r2:.4f}")

    return metrics


# ============================================
# MODEL PERSISTENCE
# ============================================

def save_model_and_metadata(model: object, metadata: Dict,
                            model_type: str, target: str) -> str:
    """Save trained model and metadata to disk"""

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    model_name = f"{CONFIG['MODEL_NAME']}_{target}_{model_type}_{timestamp}"
    model_path = os.path.join(CONFIG["MODEL_OUTPUT_PATH"], model_name)

    # Save model
    model.write().overwrite().save(model_path)

    # Save metadata
    metadata_path = os.path.join(model_path, "metadata.json")
    os.makedirs(model_path, exist_ok=True)

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\n✓ Model saved to: {model_path}")

    return model_path


# ============================================
# MAIN TRAINING PIPELINE
# ============================================

def train_energy_models(spark: SparkSession):
    """Main training pipeline for energy prediction models"""

    print("\n" + "="*60)
    print("ENERGY PREDICTION MODEL TRAINING")
    print("="*60)

    # Load data
    train_df, val_df, test_df = load_ml_dataset(spark)

    # Prepare feature columns
    feature_cols = WEATHER_FEATURES + TEMPORAL_FEATURES

    print(f"\nFeatures ({len(feature_cols)}):")
    for feat in feature_cols:
        print(f"  - {feat}")
    print(f"\nCategorical features: {CATEGORICAL_FEATURES}")

    # ========================================
    # TRAIN PRODUCTION MODEL
    # ========================================

    if CONFIG["TRAIN_PRODUCTION"]:
        print("\n" + "="*60)
        print("TRAINING PRODUCTION PREDICTION MODEL")
        print("="*60)

        pipeline, estimator = build_feature_pipeline(
            feature_cols,
            PRODUCTION_TARGET,
            include_categorical=True
        )

        # Build parameter grid
        param_grid = ParamGridBuilder() \
            .addGrid(estimator.numTrees, CONFIG["RF"]["numTrees"]) \
            .addGrid(estimator.maxDepth, CONFIG["RF"]["maxDepth"]) \
            .addGrid(estimator.minInstancesPerNode, CONFIG["RF"]["minInstancesPerNode"]) \
            .build()

        # Train
        prod_model, val_metrics = train_model_with_tuning(
            pipeline, estimator, train_df, val_df, param_grid, PRODUCTION_TARGET
        )

        # Test
        test_metrics = evaluate_on_test(prod_model, test_df, PRODUCTION_TARGET)

        # Save
        metadata = {
            "target": PRODUCTION_TARGET,
            "model_type": "RandomForest",
            "features": feature_cols,
            "categorical_features": CATEGORICAL_FEATURES,
            "validation_metrics": val_metrics,
            "test_metrics": test_metrics,
            "config": CONFIG,
            "created_at": datetime.datetime.now().isoformat()
        }

        save_model_and_metadata(prod_model, metadata, "RF", "production")

    # ========================================
    # TRAIN CONSUMPTION MODEL
    # ========================================

    if CONFIG["TRAIN_CONSUMPTION"]:
        print("\n" + "="*60)
        print("TRAINING CONSUMPTION PREDICTION MODEL")
        print("="*60)

        pipeline, estimator = build_feature_pipeline(
            feature_cols,
            CONSUMPTION_TARGET,
            include_categorical=True
        )

        # Build parameter grid
        param_grid = ParamGridBuilder() \
            .addGrid(estimator.numTrees, CONFIG["RF"]["numTrees"]) \
            .addGrid(estimator.maxDepth, CONFIG["RF"]["maxDepth"]) \
            .addGrid(estimator.minInstancesPerNode, CONFIG["RF"]["minInstancesPerNode"]) \
            .build()

        # Train
        cons_model, val_metrics = train_model_with_tuning(
            pipeline, estimator, train_df, val_df, param_grid, CONSUMPTION_TARGET
        )

        # Test
        test_metrics = evaluate_on_test(cons_model, test_df, CONSUMPTION_TARGET)

        # Save
        metadata = {
            "target": CONSUMPTION_TARGET,
            "model_type": "RandomForest",
            "features": feature_cols,
            "categorical_features": CATEGORICAL_FEATURES,
            "validation_metrics": val_metrics,
            "test_metrics": test_metrics,
            "config": CONFIG,
            "created_at": datetime.datetime.now().isoformat()
        }

        save_model_and_metadata(cons_model, metadata, "RF", "consumption")

    print("\n" + "="*60)
    print("TRAINING COMPLETE")
    print("="*60)


# ============================================
# ENTRY POINT
# ============================================

def main():
    """Main entry point"""

    spark = None
    try:
        spark = create_spark_session()
        train_energy_models(spark)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if spark:
            spark.stop()
            print("\n✓ Spark session stopped")


if __name__ == "__main__":
    main()
