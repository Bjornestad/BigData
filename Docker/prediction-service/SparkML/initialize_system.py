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
║       Phase: Aggregation & Model Training                          ║
╚════════════════════════════════════════════════════════════════════╝
""")

# Configuration
HIVE_METASTORE = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
HDFS_WAREHOUSE = os.getenv("HDFS_WAREHOUSE", "hdfs://namenode:9000/user/hive/warehouse")

def create_spark_session():
    """Create Spark session with Hive support"""
    print("\n[1/3] Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("SystemInitialization") \
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE) \
        .config("spark.sql.warehouse.dir", HDFS_WAREHOUSE) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("  ✓ Spark session initialized")
    return spark

def clean_spark_metadata(spark, table_name):
    """
    Clean _SUCCESS, _started, and other metadata files from HDFS table directory.
    These files can cause 'InvalidAvroMagicException' when Hive/Avro tries to read them.
    """
    print(f"  Cleaning Spark metadata files for {table_name}...")
    try:
        # Try to get actual table location from Hive
        try:
            # Use SQL to get location - robust against external tables
            rows = spark.sql(f"DESCRIBE FORMATTED {table_name}").collect()
            table_path_str = None
            for row in rows:
                if row['col_name'] and row['col_name'].strip() == 'Location':
                    table_path_str = row['data_type'].strip()
                    break
            
            if not table_path_str:
                table_path_str = f"{HDFS_WAREHOUSE}/{table_name}"
        except Exception:
            # Fallback if table doesn't exist or describe fails
            table_path_str = f"{HDFS_WAREHOUSE}/{table_name}"
            
        print(f"    Target path: {table_path_str}")

        sc = spark.sparkContext
        # Get Hadoop FileSystem via JVM
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        
        table_path = sc._jvm.org.apache.hadoop.fs.Path(table_path_str)
        
        if fs.exists(table_path):
            # List files/dirs in the root of the table directory
            file_statuses = fs.listStatus(table_path)
            deleted_count = 0
            for file_status in file_statuses:
                path = file_status.getPath()
                name = path.getName()
                # Delete _SUCCESS, _started, and any hidden files/dirs (starting with . or _)
                if name.startswith("_") or name.startswith("."):
                    if fs.delete(path, True): # Recursive delete
                        deleted_count += 1
            
            if deleted_count > 0:
                print(f"    ✓ Deleted {deleted_count} metadata/hidden files")
    except Exception as e:
        print(f"    ⚠️  Failed to clean metadata (non-fatal): {e}")

def create_hourly_weather_aggregates(spark):
    """Create hourly weather aggregates from raw parameters"""
    print("\n[2/3] Creating hourly weather aggregates...")

    try:
        # Clean metadata files that might confuse Avro reader
        clean_spark_metadata(spark, "weather_raw_avro")

        # Repair table to ensure partitions are discovered
        print("  Repairing table partitions for weather_raw_avro...")
        spark.sql("MSCK REPAIR TABLE weather_raw_avro")

        # Check if weather_raw_avro has data
        try:
            count = spark.sql("SELECT COUNT(*) as count FROM weather_raw_avro").collect()[0]['count']
            print(f"  Found {count:,} raw weather observations")
        except Exception as e:
            print(f"    ⚠️  Error counting weather_raw_avro (likely corrupted files): {e}")
            print("    Proceeding with available data due to ignoreCorruptFiles=true")

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
    print("\n[3/3] Training ML model...")

    try:
        # Check if we have enough data
        weather_count = spark.sql("SELECT COUNT(*) as count FROM weather_area_hourly_historical").collect()[0]['count']
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



def main():
    """Main initialization flow"""
    start_time = time.time()

    try:
        # Step 1: Create Spark session
        spark = create_spark_session()

        # Step 2: Create hourly weather aggregates
        create_hourly_weather_aggregates(spark)

        # Step 3: Train model
        model_path = train_model(spark)

        spark.stop()

        elapsed = time.time() - start_time
        print(f"\n{'=' * 70}")
        print(f"INITIALIZATION COMPLETE in {elapsed/60:.1f} minutes")
        print(f"{'=' * 70}\n")

    except Exception as e:
        print(f"\n✗ INITIALIZATION FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
