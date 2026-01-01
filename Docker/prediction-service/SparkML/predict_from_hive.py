#!/usr/bin/env python3
"""
Hive-based Energy Prediction Service
Reads latest weather data from Hive, makes predictions, publishes to Kafka
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import pyspark.sql.functions as F

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "energy_predictions")
PRODUCTION_MODEL_PATH = os.getenv("PRODUCTION_MODEL_PATH", "./SparkML/models/production_model")
CONSUMPTION_MODEL_PATH = os.getenv("CONSUMPTION_MODEL_PATH", "./SparkML/models/consumption_model")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "3600"))  # Check every hour

print(f"""
{'='*70}
HIVE-BASED ENERGY PREDICTION SERVICE
{'='*70}
Hive Table: weather_wind_solar_area_hourly_realtime
Output Topic: {OUTPUT_TOPIC}
Production Model: {PRODUCTION_MODEL_PATH}
Consumption Model: {CONSUMPTION_MODEL_PATH}
Check Interval: {CHECK_INTERVAL}s ({CHECK_INTERVAL/60:.0f} min)
{'='*70}
""")

# Initialize Spark with Hive support
print("Initializing Spark session with Hive support...")
spark = SparkSession.builder \
    .appName("HiveBasedEnergyPrediction") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hadoop.security.authentication", "simple") \
    .config("spark.hadoop.hadoop.security.authorization", "false") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .enableHiveSupport() \
    .master("local[*]") \
    .getOrCreate()

# Disable checksum verification for local file system
spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl.disable.cache", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.checksum.disabled", "true")

spark.sparkContext.setLogLevel("WARN")

# Test Hive connection
try:
    print("Checking Hive databases...")
    spark.sql("SHOW DATABASES").show()
    print("Checking Hive tables in default...")
    spark.sql("SHOW TABLES IN default").show()
    print(" Hive connection established")
except Exception as e:
    print(f" Failed to connect to Hive: {e}")
    sys.exit(1)

# Load models
print("\nLoading ML models...")
try:
    production_model = PipelineModel.load(PRODUCTION_MODEL_PATH)
    print(f"   Production model loaded")
except Exception as e:
    print(f"   Failed to load production model: {e}")
    sys.exit(1)

try:
    consumption_model = PipelineModel.load(CONSUMPTION_MODEL_PATH)
    print(f"   Consumption model loaded")
except Exception as e:
    print(f"   Failed to load consumption model: {e}")
    sys.exit(1)

# Initialize Kafka producer
print("\nConnecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    print(f"✓ Connected to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"✓ Publishing predictions to: {OUTPUT_TOPIC}")
except Exception as e:
    print(f"✗ Failed to connect to Kafka: {e}")
    sys.exit(1)

# Track processed hours to avoid duplicates
processed_hours = set()

def get_latest_weather_from_hive():
    """
    Read the latest hour of weather data from Hive
    Returns DataFrame with weather features for prediction
    """
    try:
        # Query the Hive table for latest weather data
        print("\n  Querying Hive table: weather_wind_solar_area_hourly_realtime...")

        # Get the latest hour from Hive table
        query = """
        SELECT *
        FROM weather_wind_solar_area_hourly_realtime
        ORDER BY year DESC, month DESC, day DESC, hour DESC
        LIMIT 2
        """

        latest_df = spark.sql(query)

        if latest_df.count() == 0:
            print("   No data in Hive table yet")
            return None

        count = latest_df.count()
        if count == 0:
            print("   No weather data available")
            return None

        # Show what we found
        print(f"  Found {count} latest weather records")
        latest_df.select("dk_area", "year", "month", "day", "hour", "wind_speed_avg").show()

        # Add derived features needed by ML model
        latest_df = latest_df.withColumn("month_of_year", F.col("month")) \
                           .withColumn("hour_of_day", F.col("hour"))

        return latest_df

    except Exception as e:
        print(f"  Error reading from Hive/HDFS: {e}")
        import traceback
        traceback.print_exc()
        return None

def make_predictions(weather_df):
    """
    Make predictions for each DK area in the weather DataFrame
    """
    if weather_df is None or weather_df.count() == 0:
        return []

    predictions = []

    # Process each row (should be one per DK area)
    for row in weather_df.collect():
        try:
            dk_area = row['dk_area']
            year = row['year']
            month = row['month']
            day = row['day']
            hour = row['hour']

            # Create unique key to track if we've already processed this hour
            hour_key = f"{dk_area}_{year}_{month}_{day}_{hour}"

            if hour_key in processed_hours:
                print(f"  Already processed {hour_key}, skipping")
                continue

            # Convert row to DataFrame for prediction
            row_dict = row.asDict()

            # Replace None values with appropriate defaults to avoid schema inference issues
            for key, value in row_dict.items():
                if value is None:
                    if key in ['wind_speed_avg', 'wind_speed_max', 'solar_radiation_1h',
                               'cloud_cover_avg', 'wind_direction_sin', 'wind_direction_cos',
                               'sunshine_duration_1h']:
                        row_dict[key] = 0.0
                    else:
                        row_dict[key] = 0

            df_single = spark.createDataFrame([row_dict])

            # Make production prediction
            prod_pred = production_model.transform(df_single)
            production_mwh = float(prod_pred.select("prediction").first()[0])

            # Make consumption prediction
            cons_pred = consumption_model.transform(df_single)
            consumption_mwh = float(cons_pred.select("prediction").first()[0])

            # Build prediction result
            prediction = {
                'timestamp': datetime.utcnow().isoformat(),
                'dk_area': dk_area,
                'year': int(year),
                'month': int(month),
                'day': int(day),
                'hour': int(hour),
                'predictions': {
                    'production_mwh': production_mwh,
                    'consumption_mwh': consumption_mwh,
                    'net_balance_mwh': production_mwh - consumption_mwh
                }
            }

            predictions.append((hour_key, prediction))

        except Exception as e:
            print(f"  ✗ Error making prediction for {dk_area}: {e}")
            import traceback
            traceback.print_exc()
            continue

    return predictions

def publish_predictions(predictions):
    """
    Publish predictions to Kafka
    """
    for hour_key, prediction in predictions:
        try:
            # Create Kafka key
            key = f"{prediction['dk_area']}_{prediction['year']}_{prediction['month']}_{prediction['day']}_{prediction['hour']}"

            # Publish to Kafka
            future = producer.send(OUTPUT_TOPIC, key=key, value=prediction)
            future.get(timeout=10)

            print(f"  ✓ Published prediction for {prediction['dk_area']} {prediction['year']}-{prediction['month']:02d}-{prediction['day']:02d} {prediction['hour']:02d}:00")
            print(f"      Production: {prediction['predictions']['production_mwh']:,.2f} MWh")
            print(f"      Consumption: {prediction['predictions']['consumption_mwh']:,.2f} MWh")
            print(f"      Net Balance: {prediction['predictions']['net_balance_mwh']:+,.2f} MWh")

            # Mark as processed
            processed_hours.add(hour_key)

        except Exception as e:
            print(f"  ✗ Error publishing prediction: {e}")

    producer.flush()

# Main loop
print(f"\n{'='*70}")
print("READY FOR PREDICTIONS - Monitoring Hive for new weather data...")
print(f"{'='*70}\n")

iteration = 0
try:
    while True:
        iteration += 1
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Check #{iteration}")
        print("="*70)

        # Step 1: Get latest weather from Hive
        print("Step 1: Reading latest weather data from HDFS...")
        weather_df = get_latest_weather_from_hive()

        if weather_df is not None and weather_df.count() > 0:
            # Step 2: Make predictions
            print("\nStep 2: Making predictions...")
            predictions = make_predictions(weather_df)

            if predictions:
                # Step 3: Publish to Kafka
                print(f"\nStep 3: Publishing {len(predictions)} predictions to Kafka...")
                publish_predictions(predictions)
                print(f"\n✓ Iteration {iteration} complete - {len(predictions)} new predictions")
            else:
                print(f"\n  No new predictions to publish (already processed)")
        else:
            print(f"\n  No new weather data available yet")

        # Wait for next check
        print(f"\n  Sleeping for {CHECK_INTERVAL}s ({CHECK_INTERVAL/60:.0f} min)...")
        time.sleep(CHECK_INTERVAL)

except KeyboardInterrupt:
    print("\n\n  Shutting down prediction service...")
except Exception as e:
    print(f"\n Fatal error: {e}")
    import traceback
    traceback.print_exc()
finally:
    if 'producer' in globals():
        producer.close()
    if 'spark' in globals():
        spark.stop()
    print(" Service stopped")
