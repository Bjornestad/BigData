#!/usr/bin/env python3
"""
Combined Weather Aggregation and Energy Prediction Service
1. Aggregates weather_raw_avro (long format) to weather_area_hourly (wide format)
2. Makes energy predictions if new aggregated data was created
3. Runs every 10 minutes
"""
import os
import sys
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, count, sin, cos, radians, lit
from pyspark.sql.functions import to_timestamp, year as pyspark_year, month as pyspark_month, dayofmonth, hour as pyspark_hour
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import pyspark.sql.functions as F

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "energy-platform-energy-cluster:9092")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "energy_predictions")
# Updated default path to match where Training Job saves the "latest" model
CONSUMPTION_MODEL_PATH = os.getenv("CONSUMPTION_MODEL_PATH", "/data/SparkML/models/energy_model_consumption")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))  # 10 minutes

print(f"""
{'='*70}
COMBINED 10-MIN AGGREGATION & PREDICTION SERVICE
{'='*70}
Source: weather_raw_avro (long format)
Destination: weather_area_10min (10-min buckets, by DK area)
Predictions: {OUTPUT_TOPIC} (6 predictions per hour)
Model: {CONSUMPTION_MODEL_PATH}
Check Interval: {CHECK_INTERVAL}s ({CHECK_INTERVAL/60:.0f} min)
{'='*70}
""")

# Initialize Spark with Hive support
print("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("AggregateAndPredict") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark session initialized\n")

# Station to DK area mapping
DK1_STATIONS = [
    "05005", "05009", "05015", "05031", "05035", "05042", "05065", "05070",
    "05075", "05081", "05085", "05089", "05095", "05105", "05109", "05135",
    "05140", "05150", "05160", "05165", "05169", "05185", "05199", "05202",
    "05205", "05220", "05225", "05269", "05272", "05276", "05277", "05290",
    "05296", "05300", "05305", "05320", "05329", "05343", "05345", "05350",
    "05355", "05365", "05375", "05381", "05384", "05395", "05400", "05406",
    "05408", "05435", "05440", "05450", "05455", "05469",
    "06018", "06019", "06023", "06030", "06031", "06032", "06034", "06041",
    "06043", "06049", "06051", "06052", "06056", "06058", "06060", "06065",
    "06068", "06069", "06070", "06071", "06072", "06073", "06074", "06079",
    "06080", "06081", "06082", "06088", "06089", "06093", "06096", "06102",
    "06104", "06108", "06109", "06110", "06111", "06116", "06118", "06119",
    "06120", "06123", "06124", "06126", "06132"
]

DK2_STATIONS = [
    "05499", "05505", "05510", "05529", "05537", "05545", "05575", "05735",
    "05880", "05889", "05935", "05945", "05960", "05970", "05981", "05986",
    "05994",
    "06135", "06136", "06138", "06141", "06147", "06149", "06151", "06154",
    "06156", "06159", "06168", "06169", "06170", "06174", "06180", "06181",
    "06183", "06186", "06187", "06188", "06190", "06191", "06193", "06197"
]

def station_to_dk_area(station_id):
    if station_id in DK1_STATIONS:
        return "DK1"
    elif station_id in DK2_STATIONS:
        return "DK2"
    return "UNKNOWN"

station_to_dk_area_udf = udf(station_to_dk_area, StringType())

# Load consumption model
print("Loading consumption model...")
try:
    consumption_model = PipelineModel.load(CONSUMPTION_MODEL_PATH)
    print("✓ Consumption model loaded\n")
except Exception as e:
    print(f"✗ Failed to load model: {e}")
    # We don't exit here to allow the service to keep running and retrying aggregation
    # But we won't be able to predict until model is loaded
    consumption_model = None

# Initialize Kafka producer
print("Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    print(f"✓ Connected to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"✓ Publishing to: {OUTPUT_TOPIC}\n")
except Exception as e:
    print(f"✗ Failed to connect to Kafka: {e}")
    sys.exit(1)

# Track processed 10-minute buckets
processed_buckets = set()

def aggregate_latest_10min():
    """
    Aggregate the latest 10-minute bucket from weather_raw_avro table
    Returns: True if new data was aggregated, False otherwise
    """
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking for new weather data...")

    try:
        # Read from Hive table
        raw_df = spark.sql("SELECT * FROM weather_raw_avro")

        # Get latest observed timestamp
        latest_observed = raw_df.select(spark_max(col("timeObserved")).alias("latest")).collect()[0]["latest"]

        if not latest_observed:
            print("  ⚠️  No observations found")
            return False

        # Parse the latest observation timestamp
        # Format from Connect is likely ISO8601 string
        from datetime import datetime as dt
        # Handle potential Z at end or not
        latest_observed_clean = latest_observed.replace('Z', '')
        try:
            latest_dt = dt.fromisoformat(latest_observed_clean)
        except ValueError:
             # Fallback for other formats
             latest_dt = dt.strptime(latest_observed_clean, "%Y-%m-%dT%H:%M:%S")

        # Calculate current 10-minute bucket
        current_bucket = (latest_dt.minute // 10) * 10

        # Calculate the bucket from 30 minutes ago (3 buckets back)
        # E.g., if current time is 21:50, aggregate 21:20 bucket (21:20:00-21:29:59)
        # This ensures the bucket has fully completed and all data has arrived
        bucket_dt = latest_dt.replace(minute=current_bucket, second=0, microsecond=0) - timedelta(minutes=30)

        year = bucket_dt.year
        month = bucket_dt.month
        day = bucket_dt.day
        hour = bucket_dt.hour
        minute_bucket = bucket_dt.minute

        print(f"  Latest observation: {latest_observed}")
        print(f"  Aggregating bucket from 30 min ago: {year}-{month:02d}-{day:02d} {hour:02d}:{minute_bucket:02d}")

        # Check if already aggregated
        check_query = f"""
            SELECT COUNT(*) as count
            FROM weather_area_10min
            WHERE year = {year} AND month = {month} AND day = {day}
            AND hour = {hour} AND minute_bucket = {minute_bucket}
        """

        existing_count = spark.sql(check_query).collect()[0]['count']
        if existing_count > 0:
            print(f"  ⏭️  Already aggregated ({existing_count} records)")
            return False

        # Filter observations for this specific 10-minute bucket
        # Calculate end time for the bucket (handle hour rollover)
        bucket_end_dt = bucket_dt + timedelta(minutes=10)
        bucket_end_str = bucket_end_dt.isoformat()
        bucket_start_str = bucket_dt.isoformat()

        # Note: String comparison works for ISO timestamps
        bucket_df = raw_df.filter(
            (col("timeObserved") >= bucket_start_str) &
            (col("timeObserved") < bucket_end_str)
        ) \
            .withColumn("observed_ts", to_timestamp(col("timeObserved"))) \
            .withColumn("year", pyspark_year(col("observed_ts"))) \
            .withColumn("month", pyspark_month(col("observed_ts"))) \
            .withColumn("day", dayofmonth(col("observed_ts"))) \
            .withColumn("hour", pyspark_hour(col("observed_ts"))) \
            .withColumn("minute", F.minute(col("observed_ts"))) \
            .withColumn("minute_bucket", (F.col("minute") / 10).cast("int") * 10) \
            .select("stationId", "parameterId", "value", "year", "month", "day", "hour", "minute_bucket")

        obs_count = bucket_df.count()
        print(f"  ✓ Found {obs_count:,} observations")

        if obs_count == 0:
            return False

        # Pivot to wide format
        wide_df = bucket_df.groupBy("stationId", "year", "month", "day", "hour", "minute_bucket") \
            .pivot("parameterId") \
            .avg("value")

        # Add DK area
        wide_df = wide_df.withColumn("dk_area", station_to_dk_area_udf(col("stationId")))
        wide_df = wide_df.filter(col("dk_area") != "UNKNOWN")

        # Safe aggregation helpers
        def safe_avg(col_name):
            return avg(col_name) if col_name in wide_df.columns else lit(None).cast('double')

        def safe_max(col_name):
            return spark_max(col_name) if col_name in wide_df.columns else lit(None).cast('double')

        def safe_min(col_name):
            return spark_min(col_name) if col_name in wide_df.columns else lit(None).cast('double')

        # Aggregate by DK area for 10-minute bucket
        agg_df = wide_df.groupBy("dk_area", "year", "month", "day", "hour", "minute_bucket").agg(
            safe_avg("temp_dry").alias("temp_mean_area"),
            safe_max("temp_dry").alias("temp_max_area"),
            safe_min("temp_dry").alias("temp_min_area"),
            safe_avg("wind_speed").alias("wind_speed_mean_area"),
            safe_max("wind_max").alias("wind_speed_max_area"),
            (avg(sin(radians(col("wind_dir")))) if "wind_dir" in wide_df.columns else lit(None).cast('double')).alias("wind_dir_sin_area"),
            (avg(cos(radians(col("wind_dir")))) if "wind_dir" in wide_df.columns else lit(None).cast('double')).alias("wind_dir_cos_area"),
            safe_avg("sun_last10min_glob").alias("sun_last10min_glob_area"),
            safe_avg("precip_past10min").alias("precip_past10min_mean_area"),
            safe_avg("humidity").alias("humidity_mean_area"),
            safe_avg("pressure_at_sea").alias("pressure_at_sea_mean_area"),
            safe_avg("cloud_cover").alias("cloud_cover_mean_area"),
            safe_avg("visibility").alias("visibility_mean_area"),
            (count("temp_dry").cast("int") if "temp_dry" in wide_df.columns else lit(0).cast("int")).alias("n_stations")
        )

        # Reorder to match table schema and add predicted=0 flag
        agg_df = agg_df.withColumn("predicted", lit(0)).select(
            "dk_area", "day", "hour", "minute_bucket",
            "temp_mean_area", "temp_max_area", "temp_min_area",
            "wind_speed_mean_area", "wind_speed_max_area",
            "wind_dir_sin_area", "wind_dir_cos_area",
            "sun_last10min_glob_area",
            "precip_past10min_mean_area",
            "humidity_mean_area", "pressure_at_sea_mean_area",
            "cloud_cover_mean_area", "visibility_mean_area",
            "n_stations",
            "predicted",
            "year", "month"
        )

        result_count = agg_df.count()
        print(f"  ✓ Generated {result_count} 10-min aggregates")

        # Write to weather_area_10min
        agg_df.write.mode("append").insertInto("weather_area_10min")

        print(f"  ✓ Written to weather_area_10min")
        print(f"    Bucket: {year}-{month:02d}-{day:02d} {hour:02d}:{minute_bucket:02d}")

        return True

    except Exception as e:
        print(f"  ✗ Error during aggregation: {e}")
        import traceback
        traceback.print_exc()
        return False

def make_predictions():
    """
    Make energy consumption predictions from latest 10-minute weather data
    """
    global consumption_model
    
    # Try to load model if not loaded
    if consumption_model is None:
        try:
            print("  Attempting to load model...")
            consumption_model = PipelineModel.load(CONSUMPTION_MODEL_PATH)
            print("  ✓ Model loaded")
        except Exception:
            print("  ⏭️  Skipping predictions (no model loaded)")
            return

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Making predictions...")

    try:
        # Get weather data that hasn't been predicted yet (not in predicted_buckets table)
        weather_df = spark.sql("""
            SELECT w.*
            FROM weather_area_10min w
            LEFT JOIN predicted_buckets p
              ON w.dk_area = p.dk_area
              AND w.year = p.year
              AND w.month = p.month
              AND w.day = p.day
              AND w.hour = p.hour
              AND w.minute_bucket = p.minute_bucket
            WHERE p.dk_area IS NULL
            ORDER BY w.year DESC, w.month DESC, w.day DESC, w.hour DESC, w.minute_bucket DESC
        """)

        if weather_df.count() == 0:
            print("  ⏭️  No unpredicted weather data available")
            return

        # Create timestamp for output
        weather_df = weather_df.withColumn(
            "timestamp",
            F.concat(
                F.lpad(F.col("year").cast("string"), 4, "0"), F.lit("-"),
                F.lpad(F.col("month").cast("string"), 2, "0"), F.lit("-"),
                F.lpad(F.col("day").cast("string"), 2, "0"), F.lit(" "),
                F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":"),
                F.lpad(F.col("minute_bucket").cast("string"), 2, "0"), F.lit(":00")
            )
        )

        new_weather = weather_df

        if new_weather.count() == 0:
            print("  ⏭️  No new weather data to process")
            return

        print(f"  ✓ Found {new_weather.count()} new weather records")

        # Add missing columns that consumption model expects (10-min table has limited features)
        # Model uses Imputer so NULL values are OK
        new_weather = new_weather.withColumn("temp_grass_mean_area", lit(None).cast('double')) \
                                 .withColumn("temp_soil_mean_area", lit(None).cast('double')) \
                                 .withColumn("wind_gust_always_past1h_max_area", lit(None).cast('double')) \
                                 .withColumn("radia_glob_past1h_area", lit(None).cast('double')) \
                                 .withColumn("sun_last1h_glob_area", lit(None).cast('double')) \
                                 .withColumn("precip_past1h_mean_area", lit(None).cast('double'))

        # Debug: Show input features
        print("\n  📊 Input features for prediction:")
        new_weather.select("dk_area", "month", "day", "hour", "minute_bucket",
                          "temp_mean_area", "wind_speed_mean_area", "n_stations").show(10, False)

        # Make predictions
        predictions_df = consumption_model.transform(new_weather)

        # Select relevant columns
        output_df = predictions_df.select(
            "dk_area",
            "year", "month", "day", "hour", "minute_bucket",
            "timestamp",
            F.col("prediction").alias("predicted_consumption")
        )

        # Publish to Kafka
        predictions_list = output_df.collect()

        for row in predictions_list:
            # Convert timestamp to ISO format for frontend compatibility
            timestamp_iso = f"{row['year']:04d}-{row['month']:02d}-{row['day']:02d}T{row['hour']:02d}:{row['minute_bucket']:02d}:00Z"

            prediction = {
                "dk_area": row["dk_area"],
                "timestamp": timestamp_iso,
                "year": row["year"],
                "month": row["month"],
                "day": row["day"],
                "hour": row["hour"],
                "minute_bucket": row["minute_bucket"],
                "value": float(row["predicted_consumption"]),  # Backend fallback
                "predictions": {
                    "consumption_mwh": float(row["predicted_consumption"]),
                    "production_mwh": 0,
                    "net_balance_mwh": 0
                },
                "model": "consumption",
                "prediction_time": datetime.now().isoformat()
            }

            key = f"{row['dk_area']}_{timestamp_iso}"

            producer.send(
                OUTPUT_TOPIC,
                key=key,
                value=prediction
            )

            print(f"  ✓ {row['dk_area']} {timestamp_iso}: {row['predicted_consumption']:.2f} MWh")

        producer.flush()
        print(f"  ✓ Published {len(predictions_list)} predictions")

        # Record predicted buckets in tracking table
        from pyspark.sql import Row
        from pyspark.sql.functions import current_timestamp

        predicted_records = [
            Row(
                dk_area=row['dk_area'],
                year=row['year'],
                month=row['month'],
                day=row['day'],
                hour=row['hour'],
                minute_bucket=row['minute_bucket']
            )
            for row in predictions_list
        ]

        if predicted_records:
            predicted_df = spark.createDataFrame(predicted_records)
            predicted_df = predicted_df.withColumn("predicted_at", current_timestamp())
            predicted_df.write.mode("append").insertInto("predicted_buckets")
            print(f"  ✓ Recorded {len(predicted_records)} buckets as predicted")

    except Exception as e:
        print(f"  ✗ Error making predictions: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main loop - aggregate and predict every 10 minutes"""
    iteration = 0

    while True:
        iteration += 1
        print(f"\n{'='*70}")
        print(f"CHECK #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")

        try:
            # Step 1: Aggregate new weather data (10-minute buckets)
            new_data_aggregated = aggregate_latest_10min()

            # Step 2: Always check for predictions (even if no new aggregation)
            # This ensures we make predictions on existing aggregated data after restart
            make_predictions()

        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()

        # Wait before next check
        print(f"\n  Sleeping for {CHECK_INTERVAL}s ({CHECK_INTERVAL/60:.0f} min)...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down...")
        spark.stop()
        producer.close()
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        producer.close()
        sys.exit(1)
