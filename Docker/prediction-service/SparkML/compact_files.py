#!/usr/bin/env python3
"""
Spark Compaction Job
Reads a Hive table backed by many small files and writes it back
out to a few larger files to improve read performance.
This version processes one partition at a time to avoid memory/network issues.
"""
import os
import sys
from pyspark.sql import SparkSession

def create_spark_session():
    """Create Spark session with Hive support"""
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
    """Get the HDFS location of a Hive table."""
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {table_name}").collect()
        for row in rows:
            if row['col_name'] and row['col_name'].strip() == 'Location':
                return row['data_type'].strip()
    except Exception as e:
        print(f"Could not describe table {table_name}: {e}")
        return None
    return None

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 compact_files.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]
    print(f"--- Starting Compaction for table: {table_name} ---")

    spark = create_spark_session()
    
    try:
        # Get HDFS location and partition columns
        table_location = get_table_location(spark, table_name)
        if not table_location:
            raise Exception(f"Could not find location for table {table_name}")

        partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
        partition_strings = [row.partition for row in partitions_df.collect()]
        
        if not partition_strings:
            print("Table is not partitioned. Compacting as a single unit.")
            # Handle non-partitioned table logic here if needed, for now we focus on the partitioned case
            df = spark.read.table(table_name).coalesce(10)
            temp_location = f"{table_location}_temp_compact"
            df.write.mode("overwrite").parquet(temp_location)
        else:
            partition_cols = [p.split('=')[0] for p in partition_strings[0].split('/')]
            print(f"  Table Location: {table_location}")
            print(f"  Found {len(partition_strings)} partitions. Processing one by one...")

            temp_location = f"{table_location}_temp_compact"
            
            # Loop through each partition
            for i, part_spec_str in enumerate(partition_strings):
                print(f"\n  Processing partition {i+1}/{len(partition_strings)}: {part_spec_str}")
                
                # Build a WHERE clause from the partition spec string
                # e.g., "year=2022/month=1/day=1" -> "year = 2022 AND month = 1 AND day = 1"
                where_clause = " AND ".join(part_spec_str.replace('/', ' AND ').split(' AND '))

                # Read only this partition
                partition_df = spark.read.table(table_name).where(where_clause)
                
                # Coalesce to 1 file per partition
                partition_df = partition_df.coalesce(1)

                # Write to the temp location, Spark will handle the partition subdirectories
                writer = partition_df.write.mode("append")
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                
                writer.parquet(temp_location)
                print(f"    ✓ Compacted and written.")

        # --- Replace old data with new ---
        print("\nReplacing old data with compacted data...")
        
        # Use the URI from the table location to get the correct FileSystem
        # This fixes the "Wrong FS" error
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        uri = sc._jvm.java.net.URI(table_location)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        
        # Delete original directory
        original_path = sc._jvm.org.apache.hadoop.fs.Path(table_location)
        temp_path = sc._jvm.org.apache.hadoop.fs.Path(temp_location)

        print(f"  Deleting original directory: {table_location}")
        if fs.exists(original_path):
            fs.delete(original_path, True)
        
        # Move new directory into place
        print(f"  Moving {temp_location} to {table_location}")
        if fs.exists(temp_path):
            fs.rename(temp_path, original_path)
        else:
            print(f"  ⚠️  Temp path {temp_location} does not exist! Something went wrong.")
        
        print("  ✓ Replacement complete.")

        # Refresh table partitions if it was partitioned
        if partition_strings:
            print("\nRefreshing table partitions...")
            try:
                spark.sql(f"MSCK REPAIR TABLE {table_name}")
                print("  ✓ Partitions refreshed.")
            except Exception as e:
                print(f"  ⚠️  MSCK REPAIR failed (might be non-partitioned table): {e}")

        print(f"\n--- Compaction for table {table_name} successful! ---")

    except Exception as e:
        print(f"\n--- Compaction failed for table {table_name} ---")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
