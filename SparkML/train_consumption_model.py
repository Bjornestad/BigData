#!/usr/bin/env python3
"""
Train energy consumption prediction model using hourly weather data with 10-minute parameters
"""
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour as spark_hour, dayofweek, dayofyear,
    sin, cos, lit, when, lag, avg as sql_avg, radians, concat, lpad
)
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import math

# Configuration
MODEL_OUTPUT_PATH = "hdfs://namenode:9000/user/hive/warehouse/models"
MODEL_NAME = "energy_consumption_model"

# Weather features - using actual 10-minute parameter names
WEATHER_FEATURES = [
    "temp_dry",
    "temp_dew",
    "temp_grass",
    "temp_soil",
    "humidity",
    "pressure",
    "pressure_at_sea",
    "wind_dir_sin_area",
    "wind_dir_cos_area",
    "wind_speed",
    "wind_max",
    "wind_min",
    "precip_past10min",
    "precip_dur_past10min",
    "visibility",
    "visib_mean_last10min",
    "cloud_cover",
    "cloud_height",
    "weather",
    "radia_glob",
    "radia_glob_past1h",
    "sun_last10min_glob",
    "n_stations"
]

# Temporal features - cyclical encoding + day of week
TEMPORAL_FEATURES = [
    "hour_sin",
    "hour_cos",
    "day_of_year_sin",
    "day_of_year_cos",
    "day_of_week",
    "is_weekend",
    "month",
    "day"
]

# Lag features for consumption
# NOTE: Energy API has 3-day delay, so we can't use t-24 in real-time
# We use t-72 (3 days ago) and t-168 (7 days ago) which are available
LAG_FEATURES = [
    "load_lag_72",     # Same hour 3 days ago (available in real-time)
    "load_lag_168",    # Same hour last week (available in real-time)
    "load_mean_72h",   # Rolling 72h mean (3 days)
    "load_mean_7d"     # Rolling 7 day mean
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
        .appName("TrainConsumptionModel10Min") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .enableHiveSupport() \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_and_join_data(spark):
    """
    Load historical 10-minute weather and consumption data, join by timestamp and area
    """
    print("\n" + "="*70)
    print("LOADING DATA")
    print("="*70)

    # Load historical hourly weather data from Hive table
    # Using aggregated column names from weather_area_hourly table
    print("\nLoading historical hourly weather data...")
    weather_df = spark.sql("""
        SELECT
            dk_area,
            year,
            month,
            day,
            hour,
            temp_mean_area as temp_dry,
            temp_min_area as temp_dew,
            temp_grass_mean_area as temp_grass,
            temp_soil_mean_area as temp_soil,
            humidity_mean_area as humidity,
            pressure_at_sea_mean_area as pressure,
            pressure_at_sea_mean_area as pressure_at_sea,
            wind_dir_sin_area,
            wind_dir_cos_area,
            wind_speed_mean_area as wind_speed,
            wind_speed_max_area as wind_max,
            wind_speed_mean_area as wind_min,
            precip_past10min_mean_area as precip_past10min,
            precip_past10min_mean_area as precip_dur_past10min,
            visibility_mean_area as visibility,
            visibility_mean_area as visib_mean_last10min,
            cloud_cover_mean_area as cloud_cover,
            cloud_cover_mean_area as cloud_height,
            cloud_cover_mean_area as weather,
            radia_glob_past1h_area as radia_glob,
            radia_glob_past1h_area as radia_glob_past1h,
            sun_last10min_glob_area as sun_last10min_glob,
            CAST(n_stations AS INT) as n_stations
        FROM weather_area_hourly
    """)

    weather_count = weather_df.count()
    print(f"  Weather records: {weather_count:,}")

    # Load consumption data (hourly aggregated)
    print("\nLoading hourly consumption data...")
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

    # Join on timestamp (hour level - multiple weather records per consumption)
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
                     'temp_dry', 'wind_speed', 'consumption_mwh_area').show(5)

    return joined_df

def engineer_features(df):
    """
    Add temporal and lag features to improve model accuracy
    """
    print("\n" + "="*70)
    print("FEATURE ENGINEERING")
    print("="*70)

    print("\n1. Adding cyclical time encodings...")
    # Cyclical encoding for hour (0-23)
    df = df.withColumn("hour_sin", sin(col("hour") * (2 * math.pi / 24)))
    df = df.withColumn("hour_cos", cos(col("hour") * (2 * math.pi / 24)))

    # Cyclical encoding for day of year (1-365)
    df = df.withColumn("date_col",
        concat(
            col("year").cast("string"),
            lit("-"),
            lpad(col("month").cast("string"), 2, "0"),
            lit("-"),
            lpad(col("day").cast("string"), 2, "0")
        ).cast("date")
    )
    df = df.withColumn("doy", dayofyear(col("date_col")))
    df = df.withColumn("day_of_year_sin", sin(col("doy") * (2 * math.pi / 365)))
    df = df.withColumn("day_of_year_cos", cos(col("doy") * (2 * math.pi / 365)))

    print("\n2. Adding day of week and weekend indicator...")
    df = df.withColumn("day_of_week", dayofweek(col("date_col")) - 1)  # 0=Sunday, 6=Saturday
    df = df.withColumn("is_weekend", when((col("day_of_week") == 0) | (col("day_of_week") == 6), 1).otherwise(0))

    print("\n3. Adding lag features (consumption t-72, t-168)...")
    print("   NOTE: Energy API has 3-day delay, using t-72 and t-168")
    print("   Consumption is hourly, so lags are in hours (not 10-min buckets)")
    # Define window spec partitioned by dk_area, ordered by time
    # Since consumption is hourly and we're training on hourly data:
    # - lag 72 = 3 days ago same hour (available in real-time due to API delay)
    # - lag 168 = last week same hour (24 * 7)
    window_72h = Window.partitionBy("dk_area").orderBy("year", "month", "day", "hour").rowsBetween(-72, -1)
    window_7d = Window.partitionBy("dk_area").orderBy("year", "month", "day", "hour").rowsBetween(-168, -1)

    # Lag features (hourly granularity)
    df = df.withColumn("load_lag_72", lag("consumption_mwh_area", 72).over(
        Window.partitionBy("dk_area").orderBy("year", "month", "day", "hour")
    ))
    df = df.withColumn("load_lag_168", lag("consumption_mwh_area", 168).over(
        Window.partitionBy("dk_area").orderBy("year", "month", "day", "hour")
    ))

    print("\n4. Adding rolling mean features (72h, 7d)...")
    # Rolling means (72 hours = 72 rows, 7 days = 168 rows)
    df = df.withColumn("load_mean_72h", sql_avg("consumption_mwh_area").over(window_72h))
    df = df.withColumn("load_mean_7d", sql_avg("consumption_mwh_area").over(window_7d))

    # Drop temporary columns
    df = df.drop("doy", "date_col")

    # Show sample with new features
    print("\nSample with engineered features:")
    df.select("dk_area", "year", "month", "day", "hour",
              "hour_sin", "hour_cos", "day_of_week", "is_weekend",
              "load_lag_72", "load_mean_72h", "consumption_mwh_area").show(5)

    print(f"\n✓ Feature engineering complete")
    print(f"  - Cyclical time encodings: hour_sin/cos, day_of_year_sin/cos")
    print(f"  - Day of week: day_of_week (0-6), is_weekend (0/1)")
    print(f"  - Lag features: load_lag_72 (3 days), load_lag_168 (7 days)")
    print(f"  - Rolling means: load_mean_72h (3 days), load_mean_7d (7 days)")
    print(f"\n📌 NOTE FOR PREDICTION:")
    print(f"  Energy API has 3-day delay - we use consumption from 3+ days ago")
    print(f"  When predicting every 10 minutes, use the SAME hourly consumption lag")
    print(f"  for all 6 predictions within that hour (minute_bucket 0,10,20,30,40,50)")
    print(f"  Example: Predicting at 14:00 on Jan 2:")
    print(f"           load_lag_72  = consumption on Dec 30 at hour=14")
    print(f"           load_lag_168 = consumption on Dec 26 at hour=14")

    return df

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

    # Prepare feature vector - weather + temporal + lag features
    all_features = WEATHER_FEATURES + TEMPORAL_FEATURES + LAG_FEATURES

    print(f"\nUsing {len(all_features)} features:")
    print(f"  - {len(WEATHER_FEATURES)} weather features (10-min parameters)")
    print(f"  - {len(TEMPORAL_FEATURES)} temporal features (cyclical + day of week)")
    print(f"  - {len(LAG_FEATURES)} lag features (consumption history)")

    # Build pipeline with Imputer to handle NULL values
    # Impute weather features AND lag features (first few rows will have NULL lags)
    impute_cols = WEATHER_FEATURES + LAG_FEATURES
    imputer = Imputer(
        inputCols=impute_cols,
        outputCols=[f"{c}_imputed" for c in impute_cols],
        strategy="mean"
    )

    # Use imputed features + temporal features (temporal features don't need imputation)
    imputed_features = [f"{c}_imputed" for c in impute_cols] + TEMPORAL_FEATURES

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
        predictionCol="prediction",
        numTrees=50,       # More trees for better accuracy
        maxDepth=10,       # Deeper trees to capture patterns
        seed=42
    )

    pipeline = Pipeline(stages=[imputer, assembler, scaler, rf])

    # Train
    print("\nTraining Random Forest model...")
    print(f"  Parameters: {rf.getNumTrees()} trees, max depth {rf.getMaxDepth()}")
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

    evaluator_mae = RegressionEvaluator(
        labelCol="consumption_mwh_area",
        predictionCol="prediction",
        metricName="mae"
    )

    val_rmse = evaluator_rmse.evaluate(val_predictions)
    val_r2 = evaluator_r2.evaluate(val_predictions)
    val_mae = evaluator_mae.evaluate(val_predictions)

    print(f"  Validation RMSE: {val_rmse:,.2f} MWh")
    print(f"  Validation MAE: {val_mae:,.2f} MWh")
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

    mae_evaluator = RegressionEvaluator(
        labelCol="consumption_mwh_area",
        predictionCol="prediction",
        metricName="mae"
    )

    test_rmse = rmse_evaluator.evaluate(predictions)
    test_r2 = r2_evaluator.evaluate(predictions)
    test_mae = mae_evaluator.evaluate(predictions)

    print(f"\nTest Set Results:")
    print(f"  RMSE: {test_rmse:,.2f} MWh")
    print(f"  MAE: {test_mae:,.2f} MWh")
    print(f"  R²: {test_r2:.4f}")

    print("\nSample predictions:")
    predictions.select('dk_area', 'year', 'month', 'day', 'hour',
                       'consumption_mwh_area', 'prediction').show(10)

    return test_rmse, test_r2, test_mae

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
    print("ENERGY CONSUMPTION PREDICTION MODEL TRAINING (10-MINUTE)")
    print("="*70)
    print(f"Start time: {datetime.datetime.now()}")
    print(f"\nFeatures: {len(WEATHER_FEATURES)} weather + {len(TEMPORAL_FEATURES)} temporal + {len(LAG_FEATURES)} lag")

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load and join data
        df = load_and_join_data(spark)

        # Engineer features (temporal + lag)
        df = engineer_features(df)

        # Split data
        train_df, val_df, test_df = split_data(df)

        # Build and train model
        model = build_and_train_model(train_df, val_df)

        # Evaluate on test set
        test_rmse, test_r2, test_mae = evaluate_model(model, test_df)

        # Save model
        model_path = save_model(model)

        print("\n" + "="*70)
        print("TRAINING COMPLETE")
        print("="*70)
        print(f"\nFinal Model:")
        print(f"  Path: {model_path}")
        print(f"  Test RMSE: {test_rmse:,.2f} MWh")
        print(f"  Test MAE: {test_mae:,.2f} MWh")
        print(f"  Test R²: {test_r2:.4f}")
        print(f"\nFeatures used: {len(WEATHER_FEATURES)} weather + {len(TEMPORAL_FEATURES)} temporal + {len(LAG_FEATURES)} lag")
        print(f"End time: {datetime.datetime.now()}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
