#!/usr/bin/env python3
"""
Complete system initialization script for BigData energy prediction system.
This script:
1. Creates all required Hive tables
2. Fetches historical weather and energy data
3. Creates hourly aggregated weather data
4. Trains the ML model
5. Starts the aggregate and predict service

Run this in a Kubernetes pod with access to Hive, HDFS, and Kafka.
"""
import os
import sys
import time
import subprocess
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

print("""
╔════════════════════════════════════════════════════════════════════╗
║       BIGDATA ENERGY PREDICTION SYSTEM INITIALIZATION              ║
║       This will set up all tables, fetch data, and train model     ║
╚════════════════════════════════════════════════════════════════════╝
""")

# Configuration
HIVE_METASTORE = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
HDFS_WAREHOUSE = os.getenv("HDFS_WAREHOUSE", "hdfs://namenode:9000/user/hive/warehouse")
FETCH_HISTORICAL_DAYS = int(os.getenv("FETCH_HISTORICAL_DAYS", "180"))  # 6 months default

def create_spark_session():
    """Create Spark session with Hive support"""
    print("\n[1/6] Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("SystemInitialization") \
        .config("hive.metastore.uris", HIVE_METASTORE) \
        .config("spark.sql.warehouse.dir", HDFS_WAREHOUSE) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("  ✓ Spark session initialized")
    return spark

def create_hive_tables(spark):
    """Create all required Hive tables if they don't exist"""
    print("\n[2/6] Creating Hive tables...")

    # Table 1: weather_area_10min (10-minute aggregated weather by DK area)
    print("  Creating weather_area_10min...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS weather_area_10min (
            dk_area STRING COMMENT 'Denmark area: DK1 or DK2',
            day INT COMMENT 'Day of month',
            hour INT COMMENT 'Hour of day (0-23)',
            minute_bucket INT COMMENT '10-minute bucket (0, 10, 20, 30, 40, 50)',
            temp_mean_area DOUBLE COMMENT 'Mean temperature across stations (°C)',
            temp_max_area DOUBLE COMMENT 'Max temperature across stations (°C)',
            temp_min_area DOUBLE COMMENT 'Min temperature across stations (°C)',
            wind_speed_mean_area DOUBLE COMMENT 'Mean wind speed (m/s)',
            wind_speed_max_area DOUBLE COMMENT 'Max wind speed (m/s)',
            wind_dir_sin_area DOUBLE COMMENT 'Wind direction sine component',
            wind_dir_cos_area DOUBLE COMMENT 'Wind direction cosine component',
            sun_last10min_glob_area DOUBLE COMMENT 'Solar radiation last 10 min (W/m²)',
            precip_past10min_mean_area DOUBLE COMMENT 'Precipitation past 10 min (mm)',
            humidity_mean_area DOUBLE COMMENT 'Mean relative humidity (%)',
            pressure_at_sea_mean_area DOUBLE COMMENT 'Mean sea level pressure (hPa)',
            cloud_cover_mean_area DOUBLE COMMENT 'Mean cloud cover (oktas)',
            visibility_mean_area DOUBLE COMMENT 'Mean visibility (m)',
            n_stations INT COMMENT 'Number of stations contributing data',
            predicted INT COMMENT 'Flag: 0=not predicted, 1=predicted'
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET
        COMMENT 'Weather aggregated by DK area in 10-minute buckets for predictions'
    """)
    print("    ✓ weather_area_10min created/verified")

    # Table 2: consumption_area_hourly (hourly energy consumption by DK area)
    print("  Creating consumption_area_hourly...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS consumption_area_hourly (
            dk_area STRING COMMENT 'Denmark area: DK1 or DK2',
            consumption_mwh_area DOUBLE COMMENT 'Total consumption in area (MWh)',
            day INT COMMENT 'Day of month',
            hour INT COMMENT 'Hour of day (0-23)'
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET
        COMMENT 'Energy consumption aggregated by DK area and hour'
    """)
    print("    ✓ consumption_area_hourly created/verified")

    # Table 3: predicted_buckets (tracking which buckets have been predicted)
    print("  Creating predicted_buckets...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS predicted_buckets (
            dk_area STRING COMMENT 'Denmark area: DK1 or DK2',
            year INT COMMENT 'Year',
            month INT COMMENT 'Month',
            day INT COMMENT 'Day',
            hour INT COMMENT 'Hour',
            minute_bucket INT COMMENT '10-minute bucket',
            predicted_at TIMESTAMP COMMENT 'When prediction was made'
        )
        STORED AS PARQUET
        COMMENT 'Tracking table for which 10-minute buckets have been predicted'
    """)
    print("    ✓ predicted_buckets created/verified")

    # Note: weather_raw_avro and energy_actual are created automatically by Kafka Connect
    print("  ℹ️  weather_raw_avro and energy_actual will be created by Kafka Connect")

    print("  ✓ All Hive tables created/verified")

def fetch_historical_data():
    """Fetch historical weather and energy data"""
    print(f"\n[3/6] Fetching historical data ({FETCH_HISTORICAL_DAYS} days)...")

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=FETCH_HISTORICAL_DAYS)

    print(f"  Date range: {start_date.date()} to {end_date.date()}")

    # Fetch historical weather data
    print("  Fetching historical weather data...")
    try:
        result = subprocess.run(
            [
                "python3", "/app/scripts/fetch_historical_weather.py",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d")
            ],
            check=True,
            capture_output=True,
            text=True
        )
        print("    ✓ Historical weather data fetched")
    except subprocess.CalledProcessError as e:
        print(f"    ⚠️  Weather fetch failed: {e.stderr}")
        print("    Continuing anyway - some weather data may be missing")

    # Fetch historical energy data
    print("  Fetching historical energy data...")
    try:
        result = subprocess.run(
            [
                "python3", "/app/scripts/fetch_historical_energy.py",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d")
            ],
            check=True,
            capture_output=True,
            text=True
        )
        print("    ✓ Historical energy data fetched")
    except subprocess.CalledProcessError as e:
        print(f"    ⚠️  Energy fetch failed: {e.stderr}")
        print("    Continuing anyway - some energy data may be missing")

    # Wait for Kafka Connect to process data into Hive
    print("  Waiting 30 seconds for Kafka Connect to process data into Hive...")
    time.sleep(30)

    print("  ✓ Historical data fetching complete")

def create_hourly_weather_aggregates(spark):
    """Create hourly weather aggregates from raw parameters"""
    print("\n[4/6] Creating hourly weather aggregates...")

    try:
        # Check if weather_raw_avro has data
        count = spark.sql("SELECT COUNT(*) as count FROM weather_raw_avro").collect()[0]['count']
        print(f"  Found {count:,} raw weather observations")

        if count == 0:
            print("    ⚠️  No weather data found - skipping hourly aggregation")
            print("    You'll need to fetch historical data and run create_weather_area_hourly_raw_params.py manually")
            return

        # Run the aggregation script
        print("  Running create_weather_area_hourly_raw_params.py...")
        result = subprocess.run(
            ["python3", "/app/SparkML/create_weather_area_hourly_raw_params.py"],
            check=True,
            capture_output=True,
            text=True
        )

        # Show last 20 lines of output
        output_lines = result.stdout.strip().split('\n')
        for line in output_lines[-20:]:
            print(f"    {line}")

        print("  ✓ Hourly weather aggregates created")

    except subprocess.CalledProcessError as e:
        print(f"    ✗ Failed to create hourly aggregates: {e.stderr}")
        raise

def train_model(spark):
    """Train the ML model"""
    print("\n[5/6] Training ML model...")

    try:
        # Check if we have enough data
        weather_count = spark.sql("SELECT COUNT(*) as count FROM weather_area_10min").collect()[0]['count']
        consumption_count = spark.sql("SELECT COUNT(*) as count FROM consumption_area_hourly").collect()[0]['count']

        print(f"  Weather records: {weather_count:,}")
        print(f"  Consumption records: {consumption_count:,}")

        if weather_count < 1000 or consumption_count < 100:
            print("    ⚠️  Insufficient data for training (need at least 1000 weather + 100 consumption records)")
            print("    Skipping model training - you'll need to train manually later")
            return None

        # Run the training script
        print("  Running train_consumption_model.py...")
        print("  This may take 10-30 minutes depending on data size...")

        result = subprocess.run(
            ["python3", "/app/SparkML/train_consumption_model.py"],
            check=True,
            capture_output=True,
            text=True
        )

        # Show last 30 lines of output
        output_lines = result.stdout.strip().split('\n')
        for line in output_lines[-30:]:
            print(f"    {line}")

        # Extract model path from output
        model_path = None
        for line in output_lines:
            if "Model saved to:" in line:
                model_path = line.split("Model saved to:")[-1].strip()
                break

        if model_path:
            print(f"  ✓ Model trained successfully: {model_path}")
            return model_path
        else:
            print("  ⚠️  Model training completed but path not found in output")
            # Try to find the latest model
            import glob
            models = glob.glob("/app/SparkML/models/energy_consumption_model_*")
            if models:
                model_path = max(models, key=os.path.getctime)
                print(f"  Found latest model: {model_path}")
                return model_path
            return None

    except subprocess.CalledProcessError as e:
        print(f"    ✗ Model training failed: {e.stderr}")
        print("    You'll need to train the model manually before predictions will work")
        return None

def start_prediction_service(model_path):
    """Start the aggregate and predict service"""
    print("\n[6/6] Starting prediction service...")

    if model_path:
        os.environ["CONSUMPTION_MODEL_PATH"] = model_path
        print(f"  Using model: {model_path}")
    else:
        print("  ⚠️  No model available - service will fail without a trained model")
        print("  Please train a model using: python3 /app/SparkML/train_consumption_model.py")
        print("  Then restart this service with CONSUMPTION_MODEL_PATH environment variable set")
        return

    print("\n  Starting aggregate_and_predict_service.py...")
    print("  Service will run in event-driven mode, listening to Kafka topics")
    print("  Logs will appear below:\n")
    print("=" * 70)

    # Start the service (this will run indefinitely)
    try:
        subprocess.run(
            ["python3", "/app/SparkML/aggregate_and_predict_service.py"],
            check=True
        )
    except KeyboardInterrupt:
        print("\n  Service stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"\n  ✗ Service failed: {e}")
        sys.exit(1)

def main():
    """Main initialization flow"""
    start_time = time.time()

    try:
        # Step 1: Create Spark session
        spark = create_spark_session()

        # Step 2: Create Hive tables
        create_hive_tables(spark)

        # Step 3: Fetch historical data
        fetch_historical_data()

        # Step 4: Create hourly weather aggregates
        create_hourly_weather_aggregates(spark)

        # Step 5: Train model
        model_path = train_model(spark)

        # Close Spark session before starting service (service will create its own)
        spark.stop()

        elapsed = time.time() - start_time
        print(f"\n{'=' * 70}")
        print(f"INITIALIZATION COMPLETE in {elapsed/60:.1f} minutes")
        print(f"{'=' * 70}\n")

        # Step 6: Start prediction service
        start_prediction_service(model_path)

    except Exception as e:
        print(f"\n✗ INITIALIZATION FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
