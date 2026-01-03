#!/usr/bin/env python3
"""
Train energy consumption prediction model
Joins historical weather data with energy consumption data
"""
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour as spark_hour
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

# Configuration
MODEL_OUTPUT_PATH = "hdfs://namenode:9000/user/hive/warehouse/models"
MODEL_NAME = "energy_consumption_model"

# Weather features - using Hive table column names directly
WEATHER_FEATURES = [
    "temp_mean_area",
    "temp_max_area",
    "temp_min_area",
    "temp_grass_mean_area",
    "temp_soil_mean_area",
    "wind_speed_mean_area",
    "wind_speed_max_area",
    "wind_dir_sin_area",
    "wind_dir_cos_area",
    "wind_gust_always_past1h_max_area",
    "radia_glob_past1h_area",
    "sun_last1h_glob_area",
    "sun_last10min_glob_area",
    "precip_past1h_mean_area",
    "precip_past10min_mean_area",
    "humidity_mean_area",
    "pressure_at_sea_mean_area",
    "cloud_cover_mean_area",
    "visibility_mean_area",
    "n_stations"
]

# Temporal features
TEMPORAL_FEATURES = [
    "month",
    "hour",
    "day"
]

# Training/test split
TRAIN_END_YEAR = 2024
VAL_YEAR = 2025
VAL_END_MONTH = 10
TEST_YEAR = 2025
TEST_MONTH = 11

def create_spark_session():
    """Create Spark session with Hive support"""
    spark = SparkSession.builder \
        .appName("TrainConsumptionModel") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .enableHiveSupport() \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_and_join_data(spark):
    """
    Load historical weather and consumption data, join by timestamp and area
    """
    print("\n" + "="*70)
    print("LOADING DATA")
    print("="*70)

    # Load historical weather data from Hive table
    print("\nLoading historical weather data...")
    weather_df = spark.sql("""
                           SELECT
                               *,
                               CAST(n_stations AS INT) as n_stations_int
                           FROM weather_area_hourly_historical
                           """)
    # Drop original n_stations and rename casted version
    weather_df = weather_df.drop('n_stations').withColumnRenamed('n_stations_int', 'n_stations')

    weather_count = weather_df.count()
    print(f"  Weather records: {weather_count:,}")

    # Load consumption data
    print("\nLoading consumption data...")
    consumption_df = spark.sql("""
                               SELECT
                                   dk_area,
                                   year,
                                   month,
                                   day,
                                   hour,
                                   consumption_mwh_area
                               FROM consumption_area_hourly
                               """)

    consumption_count = consumption_df.count()
    print(f"  Consumption records: {consumption_count:,}")

    # Join on timestamp and area
    print("\nJoining weather and consumption data...")
    joined_df = weather_df.join(
        consumption_df,
        on=['dk_area', 'year', 'month', 'day', 'hour'],
        how='inner'
    )

    joined_count = joined_df.count()
    print(f"  Joined records: {joined_count:,}")

    # Sample 50% of data for faster training and less memory usage
    print("\nSampling 50% of data for training...")
    joined_df = joined_df.sample(fraction=0.5, seed=42)
    sampled_count = joined_df.count()
    print(f"  Sampled records: {sampled_count:,}")

    # Show sample
    print("\nSample joined data:")
    joined_df.select('dk_area', 'year', 'month', 'day', 'hour',
                     'temp_mean_area', 'wind_speed_mean_area', 'consumption_mwh_area').show(5)

    return joined_df

def split_data(df):
    """Split data into train, validation, and test sets"""
    print("\n" + "="*70)
    print("SPLITTING DATA")
    print("="*70)

    # Training: 2021-2024
    train_df = df.filter(col('year') <= TRAIN_END_YEAR)
    train_count = train_df.count()
    print(f"\nTraining set (2021-2024): {train_count:,} records")

    # Validation: Jan-Oct 2025
    val_df = df.filter(
        (col('year') == VAL_YEAR) &
        (col('month') <= VAL_END_MONTH)
    )
    val_count = val_df.count()
    print(f"Validation set (Jan-Oct 2025): {val_count:,} records")

    # Test: Nov 2025
    test_df = df.filter(
        (col('year') == TEST_YEAR) &
        (col('month') == TEST_MONTH)
    )
    test_count = test_df.count()
    print(f"Test set (Nov 2025): {test_count:,} records")

    return train_df, val_df, test_df

def build_and_train_model(train_df, val_df):
    """Build and train the ML model"""
    print("\n" + "="*70)
    print("TRAINING MODEL")
    print("="*70)

    # Prepare feature vector
    all_features = WEATHER_FEATURES + TEMPORAL_FEATURES

    print(f"\nUsing {len(all_features)} features:")
    print(f"  - {len(WEATHER_FEATURES)} weather features")
    print(f"  - {len(TEMPORAL_FEATURES)} temporal features")

    # Build pipeline with Imputer to handle NULL values
    imputer = Imputer(
        inputCols=WEATHER_FEATURES,
        outputCols=[f"{c}_imputed" for c in WEATHER_FEATURES],
        strategy="mean"
    )

    # Use imputed features + temporal features
    imputed_features = [f"{c}_imputed" for c in WEATHER_FEATURES] + TEMPORAL_FEATURES

    assembler = VectorAssembler(
        inputCols=imputed_features,
        outputCol="features_raw",
        handleInvalid="skip"  # Skip rows with remaining NULLs
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    # Random Forest Regressor
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="consumption_mwh_area",
        predictionCol="prediction"
    )

    pipeline = Pipeline(stages=[imputer, assembler, scaler, rf])

    # Set fixed hyperparameters (reduced for memory constraints)
    rf.setNumTrees(20)  # Reduced to 20 trees for lower memory usage
    rf.setMaxDepth(6)   # Reduced depth for lower memory usage

    # Train
    print("\nTraining Random Forest model...")
    model = pipeline.fit(train_df)

    # Evaluate on validation set
    print("\nEvaluating on validation set...")
    val_predictions = model.transform(val_df)

    evaluator_rmse = RegressionEvaluator(
        labelCol="consumption_mwh_area",
        predictionCol="prediction",
        metricName="rmse"
    )

    evaluator_r2 = RegressionEvaluator(
        labelCol="consumption_mwh_area",
        predictionCol="prediction",
        metricName="r2"
    )

    val_rmse = evaluator_rmse.evaluate(val_predictions)
    val_r2 = evaluator_r2.evaluate(val_predictions)

    print(f"  Validation RMSE: {val_rmse:,.2f} MWh")
    print(f"  Validation R²: {val_r2:.4f}")

    return model

def evaluate_model(model, test_df):
    """Evaluate model on test set"""
    print("\n" + "="*70)
    print("TESTING MODEL")
    print("="*70)

    predictions = model.transform(test_df)

    rmse_evaluator = RegressionEvaluator(
        labelCol="consumption_mwh_area",
        predictionCol="prediction",
        metricName="rmse"
    )

    r2_evaluator = RegressionEvaluator(
        labelCol="consumption_mwh_area",
        predictionCol="prediction",
        metricName="r2"
    )

    test_rmse = rmse_evaluator.evaluate(predictions)
    test_r2 = r2_evaluator.evaluate(predictions)

    print(f"\nTest Set Results:")
    print(f"  RMSE: {test_rmse:,.2f} MWh")
    print(f"  R²: {test_r2:.4f}")

    print("\nSample predictions:")
    predictions.select('dk_area', 'year', 'month', 'day', 'hour',
                       'consumption_mwh_area', 'prediction').show(10)

    return test_rmse, test_r2

def save_model(model):
    """Save the trained model"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = f"{MODEL_OUTPUT_PATH}/{MODEL_NAME}_{timestamp}"

    print("\n" + "="*70)
    print(f"SAVING MODEL")
    print("="*70)
    print(f"\nSaving to: {model_path}")

    model.write().overwrite().save(model_path)
    print("✓ Model saved successfully")

    return model_path

def main():
    print("\n" + "="*70)
    print("ENERGY CONSUMPTION PREDICTION MODEL TRAINING")
    print("="*70)
    print(f"Start time: {datetime.datetime.now()}")

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load and join data
        df = load_and_join_data(spark)

        # Split data
        train_df, val_df, test_df = split_data(df)

        # Build and train model
        model = build_and_train_model(train_df, val_df)

        # Evaluate on test set
        test_rmse, test_r2 = evaluate_model(model, test_df)

        # Save model
        model_path = save_model(model)

        print("\n" + "="*70)
        print("TRAINING COMPLETE")
        print("="*70)
        print(f"\nFinal Model:")
        print(f"  Path: {model_path}")
        print(f"  Test RMSE: {test_rmse:,.2f} MWh")
        print(f"  Test R²: {test_r2:.4f}")
        print(f"\nEnd time: {datetime.datetime.now()}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()