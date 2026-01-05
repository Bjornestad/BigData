#!/usr/bin/env python3
"""
Create weather_area_hourly table with RAW 10-minute parameter names
(no _area suffix, keeping original names from DMI API)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, count,
    sin, cos, radians, lit
)
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName('CreateWeatherAreaHourlyRawParams') \
    .config('hive.metastore.uris', 'thrift://hive-metastore:9083') \
    .config('spark.sql.warehouse.dir', 'hdfs://namenode:9000/user/hive/warehouse') \
    .config('spark.driver.memory', '6g') \
    .config('spark.driver.maxResultSize', '2g') \
    .config('spark.sql.shuffle.partitions', '20') \
    .config('spark.sql.autoBroadcastJoinThreshold', '-1') \
    .config('hive.exec.dynamic.partition', 'true') \
    .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("="*70)
print("⚡ Creating weather_area_hourly with RAW Parameter Names")
print("="*70)

# Drop existing table if it exists
print("\nStep 1: Dropping existing weather_area_hourly table...")
spark.sql("DROP TABLE IF EXISTS weather_area_hourly")
print("  ✓ Table dropped")

# Define Station Lists (Hardcoded to avoid dependency on missing metadata file)
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

print("\nStep 2: Creating station metadata from hardcoded lists...")
data = []
for s in DK1_STATIONS:
    data.append((s, "DK1"))
for s in DK2_STATIONS:
    data.append((s, "DK2"))

schema = StructType([
    StructField("station_id", StringType(), False),
    StructField("dk_area", StringType(), False)
])

station_metadata = spark.createDataFrame(data, schema)
print(f"  ✓ Created metadata for {station_metadata.count()} stations")

# Process historical observations (2021-2025)
print("\nStep 3: Loading historical observations (2021-2025)...")
print("  Filtering to 22 10-minute parameters...")

# Extract hour from timeObserved since it's not a column in the raw table
obs_df = spark.sql("""
    SELECT 
        stationId as station_id, 
        parameterId as parameter_id, 
        value, 
        year, 
        month, 
        day, 
        hour(to_timestamp(timeObserved)) as hour
    FROM weather_raw_historical
    WHERE year >= 2021
    AND parameterId IN (
        'temp_dry', 'temp_dew', 'temp_grass', 'temp_soil',
        'humidity', 'pressure', 'pressure_at_sea',
        'wind_dir', 'wind_speed', 'wind_max', 'wind_min',
        'precip_past10min', 'precip_dur_past10min',
        'visibility', 'visib_mean_last10min',
        'cloud_cover', 'cloud_height', 'weather',
        'radia_glob', 'radia_glob_past1h',
        'sun_last10min_glob', 'leav_hum_dur_past10min'
    )
""")

obs_count = obs_df.count()
print(f"  ✓ Loaded {obs_count:,} observations")

print("\nStep 4: Pivoting to wide format (station-hour level)...")
wide_df = obs_df.groupBy("station_id", "year", "month", "day", "hour") \
    .pivot("parameter_id") \
    .avg("value")

print("\nStep 5: Joining with station metadata...")
wide_df = wide_df.join(station_metadata, on='station_id', how='inner')
wide_count = wide_df.count()
print(f"  ✓ {wide_count:,} station-hour records")

print("\nStep 6: Aggregating by DK area and hour (keeping raw parameter names)...")

# Helper functions for safe aggregation
def safe_avg(col_name):
    return avg(col_name) if col_name in wide_df.columns else lit(None).cast('double')

def safe_max(col_name):
    return spark_max(col_name) if col_name in wide_df.columns else lit(None).cast('double')

def safe_min(col_name):
    return spark_min(col_name) if col_name in wide_df.columns else lit(None).cast('double')

# Aggregate by area, keeping original parameter names
aggregated = wide_df.groupBy('dk_area', 'year', 'month', 'day', 'hour').agg(
    # Temperature parameters (mean values)
    safe_avg('temp_dry').alias('temp_dry'),
    safe_avg('temp_dew').alias('temp_dew'),
    safe_avg('temp_grass').alias('temp_grass'),
    safe_avg('temp_soil').alias('temp_soil'),

    # Humidity and Pressure
    safe_avg('humidity').alias('humidity'),
    safe_avg('pressure').alias('pressure'),
    safe_avg('pressure_at_sea').alias('pressure_at_sea'),

    # Wind parameters (keep wind_dir for now, we'll handle direction separately if needed)
    safe_avg('wind_dir').alias('wind_dir'),
    safe_avg('wind_speed').alias('wind_speed'),
    safe_max('wind_max').alias('wind_max'),
    safe_avg('wind_min').alias('wind_min'),

    # Precipitation
    safe_avg('precip_past10min').alias('precip_past10min'),
    safe_avg('precip_dur_past10min').alias('precip_dur_past10min'),

    # Visibility
    safe_avg('visibility').alias('visibility'),
    safe_avg('visib_mean_last10min').alias('visib_mean_last10min'),

    # Cloud
    safe_avg('cloud_cover').alias('cloud_cover'),
    safe_avg('cloud_height').alias('cloud_height'),

    # Weather code
    safe_avg('weather').alias('weather'),

    # Solar/Radiation
    safe_avg('radia_glob').alias('radia_glob'),
    safe_avg('radia_glob_past1h').alias('radia_glob_past1h'),
    safe_avg('sun_last10min_glob').alias('sun_last10min_glob'),

    # Leaf humidity
    safe_avg('leav_hum_dur_past10min').alias('leav_hum_dur_past10min'),

    # Station count
    count('temp_dry').cast('int').alias('n_stations')
)

agg_count = aggregated.count()
print(f"  ✓ Generated {agg_count:,} hourly area aggregates")

print("\nStep 7: Creating Hive table...")

# Create the table first
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS weather_area_hourly (
        dk_area STRING,
        day INT,
        hour INT,
        temp_dry DOUBLE,
        temp_dew DOUBLE,
        temp_grass DOUBLE,
        temp_soil DOUBLE,
        humidity DOUBLE,
        pressure DOUBLE,
        pressure_at_sea DOUBLE,
        wind_dir DOUBLE,
        wind_speed DOUBLE,
        wind_max DOUBLE,
        wind_min DOUBLE,
        precip_past10min DOUBLE,
        precip_dur_past10min DOUBLE,
        visibility DOUBLE,
        visib_mean_last10min DOUBLE,
        cloud_cover DOUBLE,
        cloud_height DOUBLE,
        weather DOUBLE,
        radia_glob DOUBLE,
        radia_glob_past1h DOUBLE,
        sun_last10min_glob DOUBLE,
        leav_hum_dur_past10min DOUBLE,
        n_stations INT
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:9000/user/hive/warehouse/weather_area_hourly'
""")

print("  ✓ Table created")

print("\nStep 8: Writing data to HDFS...")

# Write directly to HDFS location
hdfs_path = "hdfs://namenode:9000/user/hive/warehouse/weather_area_hourly"

aggregated.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .format("parquet") \
    .save(hdfs_path)

print(f"  ✓ Written {agg_count:,} records to {hdfs_path}")

# Refresh the Hive table metadata
print("\nStep 9: Refreshing Hive table metadata...")
spark.sql("MSCK REPAIR TABLE weather_area_hourly")
print("  ✓ Hive table refreshed")

print("\n✅ Weather area hourly table created with raw parameter names!")
print("\nSummary:")
print(f"  - Source: weather_observations_historical (2021-2025)")
print(f"  - Parameters: 22 (using raw 10-min parameter names)")
print(f"  - Output: {agg_count:,} hourly aggregates")
print(f"  - Areas: DK1, DK2")
print(f"  - Table: weather_area_hourly")
print(f"\nColumn names: temp_dry, temp_dew, temp_grass, temp_soil, humidity, etc.")
print("Ready for model training!")

spark.stop()