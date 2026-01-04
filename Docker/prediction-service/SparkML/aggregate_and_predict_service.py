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
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, count, sin, cos, radians, lit
from pyspark.sql.functions import to_timestamp, year as pyspark_year, month as pyspark_month, dayofmonth, hour as pyspark_hour
from pyspark.sql.functions import dayofweek, dayofyear, when, lag, concat, lpad
from pyspark.sql.window import Window
import math
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
import threading

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "energy_predictions")
CONSUMPTION_MODEL_PATH = os.getenv("CONSUMPTION_MODEL_PATH", "hdfs://namenode:9000/user/hive/warehouse/models")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))  # 10 minutes
HDFS_WAREHOUSE = os.getenv("HDFS_WAREHOUSE", "hdfs://namenode:9000/user/hive/warehouse")
EVENT_DRIVEN_MODE = os.getenv("EVENT_DRIVEN_MODE", "true").lower() == "true"
WEATHER_INPUT_TOPIC = os.getenv("WEATHER_INPUT_TOPIC", "weather_raw_avro")
ENERGY_INPUT_TOPIC = os.getenv("ENERGY_INPUT_TOPIC", "energy_actual")

mode_str = "EVENT-DRIVEN (listens to Kafka topics)" if EVENT_DRIVEN_MODE else f"POLLING (every {CHECK_INTERVAL}s)"
print(f"""
{'='*70}
COMBINED 10-MIN AGGREGATION & PREDICTION SERVICE
{'='*70}
Mode: {mode_str}
Source: weather_raw_avro (long format)
Destination: weather_area_10min (10-min buckets, by DK area)
Predictions: {OUTPUT_TOPIC} (6 predictions per hour)
Model: {CONSUMPTION_MODEL_PATH}
Event Topics: {WEATHER_INPUT_TOPIC}, {ENERGY_INPUT_TOPIC}
Fallback Interval: {CHECK_INTERVAL}s ({CHECK_INTERVAL/60:.0f} min)
{'='*70}
""")

# Initialize Spark with Hive support
print("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("AggregateAndPredict") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.driver.memory", "3g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
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

# Load consumption model - automatically find latest if path is a directory
print("Loading consumption model...")
try:
    from py4j.protocol import Py4JJavaError

    # If CONSUMPTION_MODEL_PATH is a directory, find the latest model using Spark
    if not CONSUMPTION_MODEL_PATH.endswith("energy_consumption_model_"):
        try:
            # Use Spark's Hadoop FileSystem API to list directory
            # Get Hadoop configuration from Spark
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI(CONSUMPTION_MODEL_PATH),
                hadoop_conf
            )

            path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(CONSUMPTION_MODEL_PATH)

            # List files in directory
            if fs.exists(path):
                file_statuses = fs.listStatus(path)
                model_dirs = []

                for status in file_statuses:
                    file_path = status.getPath().toString()
                    if 'energy_consumption_model_' in file_path:
                        model_dirs.append(file_path)

                if model_dirs:
                    # Sort by name (timestamp suffix) and get latest
                    latest_model = sorted(model_dirs)[-1]
                    print(f"  Found {len(model_dirs)} models, using latest: {latest_model.split('/')[-1]}")
                    CONSUMPTION_MODEL_PATH = latest_model
                else:
                    print(f"  No models found in {CONSUMPTION_MODEL_PATH}")
                    sys.exit(1)
            else:
                print(f"  Model directory not found: {CONSUMPTION_MODEL_PATH}")
                sys.exit(1)
        except Exception as dir_error:
            print(f"  Could not list directory (assuming direct model path): {dir_error}")
            # Assume it's a direct model path and try to load it

    consumption_model = PipelineModel.load(CONSUMPTION_MODEL_PATH)
    print(f"✓ Consumption model loaded from: {CONSUMPTION_MODEL_PATH.split('/')[-1]}\n")
except Exception as e:
    print(f"✗ Failed to load model: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

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
processed_buckets_lock = threading.Lock()

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

def aggregate_latest_10min():
    """
    Aggregate the latest 10-minute bucket from weather_raw_avro table
    Returns: True if new data was aggregated, False otherwise
    """
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking for new weather data...")

    try:
        # Only repair partitions once per day (when day changes) to avoid overhead
        # Kafka Connect should auto-register partitions via hive.integration, but sometimes
        # there's a delay, so we repair once daily to catch any missed partitions
        from datetime import datetime as dt
        current_day = dt.now().day

        # Track last repair day in a global variable (persists across iterations in same process)
        global last_repair_day
        if 'last_repair_day' not in globals() or last_repair_day != current_day:
            print("  Repairing table partitions (daily check)...")
            spark.sql("MSCK REPAIR TABLE weather_raw_avro")
            last_repair_day = current_day
        else:
            print("  Skipping partition repair (already done today)")

        # Clean metadata files that might confuse Avro reader
        clean_spark_metadata(spark, "weather_raw_avro")

        # First, get the latest observation timestamp by filtering on observed timestamp
        # Only look at last 30 minutes since new data arrives every 10 minutes
        from datetime import datetime as dt
        now = dt.now()
        cutoff = (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:00Z")

        # Use partition pruning to only scan recent partitions
        cutoff_year = now.year
        cutoff_month = now.month
        cutoff_day = now.day

        latest_result = spark.sql(f"""
            SELECT MAX(timeObserved) as latest
            FROM weather_raw_avro
            WHERE year = {cutoff_year}
            AND month = {cutoff_month}
            AND day = {cutoff_day}
            AND timeObserved >= '{cutoff}'
        """).collect()[0]
        latest_observed = latest_result["latest"]

        if not latest_observed:
            print("  ⚠️  No observations found")
            return None

        # Parse the latest observation timestamp
        from datetime import datetime as dt
        latest_dt = dt.strptime(latest_observed, "%Y-%m-%dT%H:%M:%SZ")

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

        bucket_key = (year, month, day, hour, minute_bucket)

        # Prevent duplicate work while async write is still in-flight
        with processed_buckets_lock:
            if bucket_key in processed_buckets:
                print(f"  ⏭️  Bucket already in-flight/processed: {bucket_key}")
                return None
            processed_buckets.add(bucket_key)

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
            return None

        # Filter observations for this specific 10-minute bucket DIRECTLY in SQL
        # Calculate end time for the bucket (handle hour rollover)
        bucket_end_dt = bucket_dt + timedelta(minutes=10)
        bucket_end_str = bucket_end_dt.strftime("%Y-%m-%dT%H:%M:00Z")
        bucket_start_str = bucket_dt.strftime("%Y-%m-%dT%H:%M:00Z")

        # Extract partition values for efficient partition pruning
        bucket_year = bucket_dt.year
        bucket_month = bucket_dt.month
        bucket_day = bucket_dt.day

        # Read ONLY the specific 10-minute bucket from Hive - much more memory efficient
        # CRITICAL: Filter on partition columns (year, month, day) FIRST for partition pruning
        bucket_df = spark.sql(f"""
            SELECT stationId, parameterId, value, timeObserved
            FROM weather_raw_avro
            WHERE year = {bucket_year}
            AND month = {bucket_month}
            AND day = {bucket_day}
            AND timeObserved >= '{bucket_start_str}'
            AND timeObserved < '{bucket_end_str}'
        """) \
            .withColumn("observed_ts", to_timestamp(col("timeObserved"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
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
        # Use raw parameter names (no _area suffix) to match training model
        agg_df = wide_df.groupBy("dk_area", "year", "month", "day", "hour", "minute_bucket").agg(
            # Temperature parameters (matching model training)
            safe_avg("temp_dry").alias("temp_dry"),
            safe_avg("temp_dew").alias("temp_dew"),
            safe_avg("temp_grass").alias("temp_grass"),
            safe_avg("temp_soil").alias("temp_soil"),

            # Humidity and Pressure
            safe_avg("humidity").alias("humidity"),
            safe_avg("pressure").alias("pressure"),
            safe_avg("pressure_at_sea").alias("pressure_at_sea"),

            # Wind parameters with cyclical encoding
            (avg(sin(radians(col("wind_dir")))) if "wind_dir" in wide_df.columns else lit(None).cast('double')).alias("wind_dir_sin"),
            (avg(cos(radians(col("wind_dir")))) if "wind_dir" in wide_df.columns else lit(None).cast('double')).alias("wind_dir_cos"),
            safe_avg("wind_speed").alias("wind_speed"),
            safe_max("wind_max").alias("wind_max"),
            safe_avg("wind_min").alias("wind_min"),

            # Precipitation
            safe_avg("precip_past10min").alias("precip_past10min"),
            safe_avg("precip_dur_past10min").alias("precip_dur_past10min"),

            # Visibility
            safe_avg("visibility").alias("visibility"),
            safe_avg("visib_mean_last10min").alias("visib_mean_last10min"),

            # Cloud
            safe_avg("cloud_cover").alias("cloud_cover"),
            safe_avg("cloud_height").alias("cloud_height"),

            # Weather code
            safe_avg("weather").alias("weather"),

            # Solar/Radiation
            safe_avg("radia_glob").alias("radia_glob"),
            safe_avg("radia_glob_past1h").alias("radia_glob_past1h"),
            safe_avg("sun_last10min_glob").alias("sun_last10min_glob"),

            # Station count
            (count("temp_dry").cast("int") if "temp_dry" in wide_df.columns else lit(0).cast("int")).alias("n_stations")
        )

        # Add predicted=0 flag
        agg_df = agg_df.withColumn("predicted", lit(0))

        # Reorder columns to match Hive table schema (non-partition columns first, then partition columns)
        # Using raw parameter names (no _area suffix) to match model training
        agg_df = agg_df.select(
            "dk_area", "day", "hour", "minute_bucket",
            # All 23 weather parameters in same order as training model
            "temp_dry", "temp_dew", "temp_grass", "temp_soil",
            "humidity", "pressure", "pressure_at_sea",
            "wind_dir_sin", "wind_dir_cos",
            "wind_speed", "wind_max", "wind_min",
            "precip_past10min", "precip_dur_past10min",
            "visibility", "visib_mean_last10min",
            "cloud_cover", "cloud_height", "weather",
            "radia_glob", "radia_glob_past1h", "sun_last10min_glob",
            "n_stations", "predicted",
            "year", "month"
        )

        # Cache the dataframe to avoid re-computing for count, write, and prediction
        agg_df.cache()

        result_count = agg_df.count()
        print(f"  ✓ Generated {result_count} 10-min aggregates")

        # Write to weather_area_10min asynchronously (non-blocking)
        # This way the main thread can proceed with predictions immediately
        def async_write():
            try:
                # Use insertInto instead of saveAsTable to respect existing partition scheme
                agg_df.write \
                    .mode("append") \
                    .format("hive") \
                    .insertInto("weather_area_10min")
                print(f"  ✓ Written to weather_area_10min (background)")
            except Exception as e:
                print(f"  ⚠️  Background write failed: {e}")
                # allow retry if the write failed
                with processed_buckets_lock:
                    processed_buckets.discard(bucket_key)

        write_thread = threading.Thread(target=async_write, daemon=True)
        write_thread.start()

        print(f"  ✓ Started background write for bucket: {year}-{month:02d}-{day:02d} {hour:02d}:{minute_bucket:02d}")

        # Return the aggregated DataFrame so we can predict directly without re-querying
        return agg_df

    except Exception as e:
        print(f"  ✗ Error during aggregation: {e}")
        import traceback
        traceback.print_exc()
        return None

def add_temporal_and_lag_features(df):
    """
    Add temporal and lag features to match training data
    """
    # 1. Cyclical time encoding
    df = df.withColumn("hour_sin", sin(col("hour") * (2 * math.pi / 24)))
    df = df.withColumn("hour_cos", cos(col("hour") * (2 * math.pi / 24)))

    # 2. Day of year (cyclical)
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

    # 3. Day of week and weekend
    df = df.withColumn("day_of_week", dayofweek(col("date_col")) - 1)
    df = df.withColumn("is_weekend", when((col("day_of_week") == 0) | (col("day_of_week") == 6), 1).otherwise(0))

    # 4. Lag features from consumption_area_hourly table
    # For each 10-min prediction, use consumption from 3+ days ago at the SAME HOUR
    # Note: All 6 predictions in hour X use the same hourly consumption lags

    # Load consumption history (3+ days old to match API delay)
    consumption_df = spark.sql("""
        SELECT
            dk_area,
            year,
            month,
            day,
            hour,
            consumption_mwh_area
        FROM consumption_area_hourly
        WHERE consumption_mwh_area IS NOT NULL
    """)

    # Create timestamp for joining
    consumption_df = consumption_df.withColumn("cons_timestamp",
        F.unix_timestamp(
            concat(
                lpad(col("year").cast("string"), 4, "0"), lit("-"),
                lpad(col("month").cast("string"), 2, "0"), lit("-"),
                lpad(col("day").cast("string"), 2, "0"), lit(" "),
                lpad(col("hour").cast("string"), 2, "0"), lit(":00:00")
            )
        )
    )

    # Create timestamp for weather data (ignoring minute_bucket for hourly join)
    df = df.withColumn("weather_timestamp",
        F.unix_timestamp(
            concat(
                lpad(col("year").cast("string"), 4, "0"), lit("-"),
                lpad(col("month").cast("string"), 2, "0"), lit("-"),
                lpad(col("day").cast("string"), 2, "0"), lit(" "),
                lpad(col("hour").cast("string"), 2, "0"), lit(":00:00")
            )
        )
    )

    # Window for lag calculations by dk_area ordered by timestamp
    window_spec = Window.partitionBy("dk_area").orderBy("cons_timestamp")

    # Add lag features: load_lag_72 (3 days = 72 hours), load_lag_168 (7 days = 168 hours)
    consumption_df = consumption_df.withColumn("load_lag_72", lag("consumption_mwh_area", 72).over(window_spec))
    consumption_df = consumption_df.withColumn("load_lag_168", lag("consumption_mwh_area", 168).over(window_spec))

    # Rolling means
    window_72h = Window.partitionBy("dk_area").orderBy("cons_timestamp").rowsBetween(-72, -1)
    window_7d = Window.partitionBy("dk_area").orderBy("cons_timestamp").rowsBetween(-168, -1)

    consumption_df = consumption_df.withColumn("load_mean_72h", F.avg("consumption_mwh_area").over(window_72h))
    consumption_df = consumption_df.withColumn("load_mean_7d", F.avg("consumption_mwh_area").over(window_7d))

    # Join weather with consumption lags (on dk_area and matching the hour, ignoring minute_bucket)
    df = df.join(
        consumption_df.select("dk_area", "cons_timestamp", "load_lag_72", "load_lag_168", "load_mean_72h", "load_mean_7d"),
        (df.dk_area == consumption_df.dk_area) & (df.weather_timestamp == consumption_df.cons_timestamp),
        "left"
    ).drop(consumption_df.dk_area)

    # Drop temporary columns
    df = df.drop("date_col", "doy", "weather_timestamp", "cons_timestamp")

    return df

def make_predictions(new_weather_df=None):
    """
    Make energy consumption predictions from latest 10-minute weather data

    Args:
        new_weather_df: Optional DataFrame with newly aggregated weather data.
                       If provided, predictions will be made on this data directly.
                       If None, will query unpredicted data from weather_area_10min table.
    """
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Making predictions...")

    try:
        # Use provided DataFrame or query for unpredicted data
        if new_weather_df is not None:
            weather_df = new_weather_df
            print("  ✓ Using newly aggregated weather data")
        else:
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

        # Add temporal and lag features (critical for model accuracy!)
        print("\n  🔧 Adding temporal and lag features...")
        new_weather = add_temporal_and_lag_features(new_weather)

        # Debug: Show input features
        print("\n  📊 Input features for prediction:")
        new_weather.select("dk_area", "month", "day", "hour", "minute_bucket",
                          "temp_dry", "wind_speed", "hour_sin", "hour_cos",
                          "load_lag_72", "load_lag_168", "n_stations").show(10, False)

        # Make predictions
        predictions_df = consumption_model.transform(new_weather)


        # Select relevant columns
        output_df = predictions_df.select(
            "dk_area",
            "year", "month", "day", "hour", "minute_bucket",
            "timestamp",
            F.col("prediction").alias("predicted_consumption")
        )

        # Publish to Kafka - combine DK1 and DK2 into single Denmark prediction
        predictions_list = output_df.collect()

        # Group predictions by timestamp and sum consumption across DK1 and DK2
        from collections import defaultdict
        combined_predictions = defaultdict(lambda: {"consumption": 0, "year": 0, "month": 0, "day": 0, "hour": 0, "minute_bucket": 0})

        for row in predictions_list:
            timestamp_iso = f"{row['year']:04d}-{row['month']:02d}-{row['day']:02d}T{row['hour']:02d}:{row['minute_bucket']:02d}:00Z"
            combined_predictions[timestamp_iso]["consumption"] += float(row["predicted_consumption"])
            combined_predictions[timestamp_iso]["year"] = row["year"]
            combined_predictions[timestamp_iso]["month"] = row["month"]
            combined_predictions[timestamp_iso]["day"] = row["day"]
            combined_predictions[timestamp_iso]["hour"] = row["hour"]
            combined_predictions[timestamp_iso]["minute_bucket"] = row["minute_bucket"]

        # Send combined predictions
        for timestamp_iso, pred_data in combined_predictions.items():
            prediction = {
                "dk_area": "DK",  # Combined Denmark
                "timestamp": timestamp_iso,
                "year": pred_data["year"],
                "month": pred_data["month"],
                "day": pred_data["day"],
                "hour": pred_data["hour"],
                "minute_bucket": pred_data["minute_bucket"],
                "value": pred_data["consumption"],  # Backend fallback
                "predictions": {
                    "consumption_mwh": pred_data["consumption"],
                    "production_mwh": 0,
                    "net_balance_mwh": 0
                },
                "model": "consumption",
                "prediction_time": datetime.now().isoformat()
            }

            key = f"DK_{timestamp_iso}"

            producer.send(
                OUTPUT_TOPIC,
                key=key,
                value=prediction
            )

            print(f"  ✓ DK {timestamp_iso}: {pred_data['consumption']:.2f} MWh (combined)")

        producer.flush()
        print(f"  ✓ Published {len(combined_predictions)} combined predictions")

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
            # Use saveAsTable with name-based matching instead of insertInto with positional matching
            predicted_df.write.mode("append").format("hive").saveAsTable("predicted_buckets")
            print(f"  ✓ Recorded {len(predicted_records)} buckets as predicted")

    except Exception as e:
        print(f"  ✗ Error making predictions: {e}")
        import traceback
        traceback.print_exc()

def aggregate_consumption_area_hourly():
    """
    Aggregate raw consumption data from energy_actual table into consumption_area_hourly
    Processes all raw records and aggregates by PriceArea (mapped to dk_area) and hour
    """
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Aggregating consumption data...")

    try:
        # Only repair partitions once per day (when day changes) to avoid overhead
        # Kafka Connect should auto-register partitions via hive.integration
        from datetime import datetime as dt
        current_day = dt.now().day

        # Track last repair day for energy table
        global last_energy_repair_day
        if 'last_energy_repair_day' not in globals() or last_energy_repair_day != current_day:
            print("  Repairing energy_actual table partitions (daily check)...")
            spark.sql("MSCK REPAIR TABLE energy_actual")
            last_energy_repair_day = current_day
        else:
            print("  Skipping energy partition repair (already done today)")

        # Clean metadata files that might confuse Avro reader
        clean_spark_metadata(spark, "energy_actual")

        # Only read recent data (last 7 days) to avoid memory issues
        # Older data has already been aggregated in previous runs
        from datetime import datetime as dt
        cutoff_date = (dt.now() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")

        raw_consumption_df = spark.sql(f"""
            SELECT * FROM energy_actual
            WHERE HourDK >= '{cutoff_date}'
        """)

        total_records = raw_consumption_df.count()
        if total_records == 0:
            print("  ⚠️  No raw consumption data found")
            return None

        print(f"  ✓ Found {total_records:,} raw consumption records (last 7 days)")

        # Parse HourDK timestamp and extract time components
        # HourDK format: "2025-01-03T21:00:00"
        processed_df = raw_consumption_df \
            .withColumn("hour_ts", to_timestamp(col("HourDK"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("year", pyspark_year(col("hour_ts")).cast("int")) \
            .withColumn("month", pyspark_month(col("hour_ts")).cast("int")) \
            .withColumn("day", dayofmonth(col("hour_ts")).cast("int")) \
            .withColumn("hour", pyspark_hour(col("hour_ts")).cast("int"))

        # Map PriceArea to dk_area (DK1 or DK2)
        processed_df = processed_df.withColumn(
            "dk_area",
            when(col("PriceArea") == "DK1", "DK1")
            .when(col("PriceArea") == "DK2", "DK2")
            .otherwise(None)
        ).filter(col("dk_area").isNotNull())

        # Aggregate ShareMWh by dk_area and hour
        # Sum all consumption across ConnectedArea, ViaArea for each PriceArea and hour
        agg_df = processed_df.groupBy("dk_area", "year", "month", "day", "hour").agg(
            F.sum("ShareMWh").alias("consumption_mwh_area")
        )

        agg_count = agg_df.count()
        print(f"  ✓ Generated {agg_count:,} hourly aggregates")

        # Check which records already exist - use a left anti join with explicit casting
        # Create a temporary view with correct types
        agg_df.createOrReplaceTempView("temp_agg_consumption")

        new_records_df = spark.sql("""
            SELECT t.dk_area,
                   t.consumption_mwh_area,
                   t.year,
                   t.month,
                   t.day,
                   t.hour
            FROM temp_agg_consumption t
            LEFT ANTI JOIN consumption_area_hourly e
              ON t.dk_area = e.dk_area
              AND t.year = e.year
              AND t.month = e.month
              AND t.day = e.day
              AND t.hour = e.hour
        """)

        new_count = new_records_df.count()

        if new_count == 0:
            print(f"  ⏭️  All records already aggregated")
            return None

        print(f"  ✓ Found {new_count:,} new hourly records to insert")

        # Write new records asynchronously (non-blocking)
        def async_write():
            try:
                new_records_df.write \
                    .mode("append") \
                    .partitionBy("dk_area", "year", "month") \
                    .format("hive") \
                    .saveAsTable("consumption_area_hourly")
                print(f"  ✓ Written to consumption_area_hourly (background)")
            except Exception as e:
                print(f"  ⚠️  Background write to consumption_area_hourly failed: {e}")

        write_thread = threading.Thread(target=async_write, daemon=True)
        write_thread.start()

        # Show sample of newly aggregated data
        sample_records = new_records_df.orderBy(
            col("year").desc(),
            col("month").desc(),
            col("day").desc(),
            col("hour").desc()
        ).limit(5).collect()

        print(f"  ✓ Started background write for {new_count:,} new hourly records")
        print("\n  📊 Sample of newly aggregated records:")
        for row in sample_records:
            print(f"    {row['dk_area']} {row['year']}-{row['month']:02d}-{row['day']:02d} {row['hour']:02d}:00 → {row['consumption_mwh_area']:.2f} MWh")

        # Return new records so lag features can be recalculated if needed
        return new_records_df

    except Exception as e:
        print(f"  ✗ Error aggregating consumption data: {e}")
        import traceback
        traceback.print_exc()
        return None

def run_weather_pipeline():
    """Execute weather aggregation and prediction pipeline (triggered by weather events)"""
    try:
        # Step 1: Aggregate new weather data (10-minute buckets)
        new_weather_df = aggregate_latest_10min()

        # Step 2: Make predictions ONLY if new weather data was aggregated
        if new_weather_df is not None:
            make_predictions(new_weather_df)
            new_weather_df.unpersist() # Free up memory
        else:
            print("  ⏭️  No new weather aggregation, skipping predictions")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

def run_energy_pipeline():
    """Execute energy consumption aggregation (triggered by energy events)"""
    try:
        aggregate_consumption_area_hourly()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()



def kafka_event_listener():
    """Listen to Kafka topics and trigger aggregations when new data arrives"""
    print(f"\n🎧 Starting Kafka event listener...")
    print(f"   Listening to: {WEATHER_INPUT_TOPIC}, {ENERGY_INPUT_TOPIC}")

    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            WEATHER_INPUT_TOPIC,
            ENERGY_INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id='aggregate-predict-service',
            auto_offset_reset='latest',  # Only process new messages
            enable_auto_commit=True,
            consumer_timeout_ms=10000,  # 10 second timeout for checking
            value_deserializer=lambda x: x  # We don't need to deserialize, just detect new messages
        )

        print(f"   ✓ Kafka consumer connected\n")

        last_weather_trigger = 0
        last_energy_trigger = 0
        min_trigger_interval = 60  # Minimum 60 seconds between triggers to avoid excessive processing

        while True:
            # Poll for new messages
            messages = consumer.poll(timeout_ms=10000)

            if messages:
                current_time = time.time()

                # Count messages by topic
                weather_count = sum(len(msgs) for topic_partition, msgs in messages.items()
                                  if topic_partition.topic == WEATHER_INPUT_TOPIC)
                energy_count = sum(len(msgs) for topic_partition, msgs in messages.items()
                                 if topic_partition.topic == ENERGY_INPUT_TOPIC)

                # Trigger weather pipeline if weather data arrived
                if weather_count > 0:
                    if current_time - last_weather_trigger >= min_trigger_interval:
                        print(f"\n📨 New weather data detected: {weather_count} messages")
                        print(f"{'='*70}")
                        print(f"WEATHER EVENT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"{'='*70}")

                        # Run weather aggregation + prediction and reset timer
                        run_weather_pipeline_with_timer()
                        last_weather_trigger = current_time
                    else:
                        time_remaining = min_trigger_interval - (current_time - last_weather_trigger)
                        print(f"   ⏱️  Weather data arrived, waiting {time_remaining:.0f}s...")

                # Trigger energy pipeline if energy data arrived
                if energy_count > 0:
                    if current_time - last_energy_trigger >= min_trigger_interval:
                        print(f"\n📨 New energy data detected: {energy_count} messages")
                        print(f"{'='*70}")
                        print(f"ENERGY EVENT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"{'='*70}")

                        # Run energy aggregation only (predictions use lag features, don't need immediate trigger)
                        run_energy_pipeline()
                        last_energy_trigger = current_time
                    else:
                        time_remaining = min_trigger_interval - (current_time - last_energy_trigger)
                        print(f"   ⏱️  Energy data arrived, waiting {time_remaining:.0f}s...")
            else:
                # No messages, just continue listening
                pass

    except Exception as e:
        print(f"\n✗ Kafka event listener error: {e}")
        import traceback
        traceback.print_exc()
        # Fall back to polling mode if event listener fails
        raise  # Re-raise so main thread knows to fall back

# Global timer for tracking last execution
last_execution_time = 0
timer_lock = threading.Lock()

def reset_timer():
    """Reset the execution timer when pipeline runs"""
    global last_execution_time
    with timer_lock:
        last_execution_time = time.time()

def run_weather_pipeline_with_timer():
    """Execute the weather pipeline and reset timer"""
    run_weather_pipeline()
    reset_timer()

def main():
    """Main entry point - either event-driven or polling mode"""
    global last_execution_time

    if EVENT_DRIVEN_MODE:
        print(f"\n🚀 Starting in EVENT-DRIVEN mode")
        print(f"   Pipeline triggers when new data arrives in Kafka")
        print(f"   Fallback runs if no events for {CHECK_INTERVAL}s\n")

        # Start event listener in background thread
        event_thread = threading.Thread(
            target=kafka_event_listener,
            daemon=True,
            name="KafkaEventListener"
        )
        event_thread.start()

        # Initialize timer
        reset_timer()

        # Keep main thread alive with smart fallback
        iteration = 0
        while True:
            # Check how long since last execution
            with timer_lock:
                time_since_last = time.time() - last_execution_time

            # If enough time passed without events, run fallback
            if time_since_last >= CHECK_INTERVAL:
                iteration += 1
                print(f"\n{'='*70}")
                print(f"FALLBACK TRIGGER #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   (No weather events for {time_since_last:.0f}s)")
                print(f"{'='*70}")

                # Run weather pipeline as fallback
                run_weather_pipeline_with_timer()

            # Sleep for short intervals to check timer frequently
            time.sleep(10)  # Check every 10 seconds
    else:
        print(f"\n🔄 Starting in POLLING mode")
        print(f"   Weather pipeline runs every {CHECK_INTERVAL}s\n")

        iteration = 0
        while True:
            iteration += 1
            print(f"\n{'='*70}")
            print(f"CHECK #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}")

            # Run weather pipeline on schedule
            run_weather_pipeline()

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