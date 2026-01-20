#!/usr/bin/env python3
"""
Generic Spark Compaction Job for Hive Tables

Description:
  This script compacts a specified Hive table that suffers from the "small file problem."
  It reads the table, coalesces the data into a smaller number of larger files,
  and overwrites the original data. This significantly improves read performance
  for query engines like Spark, Hive, and Presto.

  The script is designed to be robust and works for both partitioned and non-partitioned tables.
  For partitioned tables, it processes one partition at a time to minimize memory usage
  and avoid out-of-memory errors on large tables.

How to Run:
  You can execute this script from within a Spark environment (like the 'aggregate-and-predict' container)
  that has access to the Hive Metastore and HDFS.

  Command:
    python3 compact_files.py <table_name>

  Examples:
    # Compact the live weather data table
    python3 compact_files.py weather_raw_avro

    # Compact the historical aggregated weather table for training
    python3 compact_files.py weather_area_hourly_historical
"""
import os
import sys
from pyspark.sql import SparkSession

# --- Configuration ---
# Number of output files for non-partitioned tables
# For very large tables, you might increase this number.
# Can be overridden with environment variable.
NUM_OUTPUT_FILES_NON_PARTITIONED = int(os.getenv("NUM_OUTPUT_FILES_NON_PARTITIONED", "10"))

# Number of output files PER PARTITION for partitioned tables.
# '1' is usually ideal to maximize file size.
NUM_OUTPUT_FILES_PER_PARTITION = int(os.getenv("NUM_OUTPUT_FILES_PER_PARTITION", "1"))

def create_spark_session():
    """Create a Spark session with Hive support."""
    metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    
    spark = SparkSession.builder \
        .appName("CompactionJob") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", metastore_uri) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_table_location(spark, table_name):
    """Get the HDFS location of a Hive table from its description."""
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {table_name}").collect()
        for row in rows:
            if row['col_name'] and row['col_name'].strip() == 'Location':
                return row['data_type'].strip()
    except Exception as e:
        print(f"✗ Could not describe table {table_name}: {e}")
        return None
    return None

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 compact_files.py <table_name>")
        print("Example: python3 compact_files.py weather_area_hourly_historical")
        sys.exit(1)

    table_name = sys.argv[1]
    print(f"\n{'='*60}\n--- Starting Compaction for table: {table_name} ---\n{'='*60}")

    spark = create_spark_session()
    
    try:
        # Step 1: Get table metadata (location and partitions)
        table_location = get_table_location(spark, table_name)
        if not table_location:
            raise Exception(f"Could not find HDFS location for table {table_name}")

        print(f"  ✓ Table Location: {table_location}")
        
        # Determine if the table is partitioned
        try:
            partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
            partition_strings = [row.partition for row in partitions_df.collect()]
        except Exception:
            partition_strings = []

        temp_location = f"{table_location}_temp_compact_{os.urandom(4).hex()}"
        print(f"  ✓ Temporary write location: {temp_location}")

        # Step 2: Read data, coalesce, and write to a temporary location
        if not partition_strings:
            # --- Logic for Non-Partitioned Tables ---
            print("\n  ⓘ Table is not partitioned. Compacting as a single unit...")
            
            df = spark.read.table(table_name)
            df = df.coalesce(NUM_OUTPUT_FILES_NON_PARTITIONED)
            
            print(f"  Coalescing into {NUM_OUTPUT_FILES_NON_PARTITIONED} file(s)...")
            df.write.mode("overwrite").parquet(temp_location)

        else:
            # --- Logic for Partitioned Tables ---
            partition_cols = [p.split('=')[0] for p in partition_strings[0].split('/')]
            print(f"\n  ⓘ Table is partitioned by: {partition_cols}")
            print(f"  Found {len(partition_strings)} partitions. Processing one by one...")

            # Set Spark config to write partitions to the root of the temp directory
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

            for i, part_spec_str in enumerate(partition_strings):
                print(f"    - Processing partition {i+1}/{len(partition_strings)}: {part_spec_str} ...")
                
                where_clause = " AND ".join(part_spec_str.replace('/', ' AND ').split(' AND '))
                partition_df = spark.read.table(table_name).where(where_clause)
                
                # Coalesce to a smaller number of files
                compacted_df = partition_df.coalesce(NUM_OUTPUT_FILES_PER_PARTITION)
                
                # Write to the temp location. Spark appends new partition directories.
                compacted_df.write.mode("append").partitionBy(*partition_cols).parquet(temp_location)
            
            print(f"  ✓ All partitions compacted into temp directory.")

        # Step 3: Replace the original table data with the compacted data
        print("\n  Replacing old data with compacted data (atomic rename)...")
        
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        uri = sc._jvm.java.net.URI(table_location)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        
        original_path = sc._jvm.org.apache.hadoop.fs.Path(table_location)
        temp_path = sc._jvm.org.apache.hadoop.fs.Path(temp_location)

        # First, delete the original directory
        print(f"    - Deleting original directory: {table_location}")
        if fs.exists(original_path):
            fs.delete(original_path, True)
        else:
            print(f"    - Warning: Original path did not exist. Creating new.")

        # Then, rename the temp directory to the original directory name
        print(f"    - Renaming {temp_location} to {table_location}")
        if fs.exists(temp_path):
            fs.rename(temp_path, original_path)
        else:
            raise Exception(f"Temp path {temp_location} does not exist! Compaction write failed.")
        
        print("  ✓ Atomic replacement complete.")

        # Step 4: Refresh the Hive Metastore to recognize the new data files
        if partition_strings:
            print("\n  Refreshing table partitions in Hive Metastore...")
            # MSCK REPAIR TABLE tells Hive to update its partition metadata
            spark.sql(f"MSCK REPAIR TABLE {table_name}")
            print("  ✓ Partitions refreshed.")

        print(f"\n{'='*60}\n--- Compaction for table '{table_name}' successful! ---\n{'='*60}")

    except Exception as e:
        print(f"\n--- ✗ Compaction failed for table '{table_name}' ---")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()