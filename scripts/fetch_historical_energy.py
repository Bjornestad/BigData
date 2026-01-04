#!/usr/bin/env python3
"""
Historical Energy Data Fetcher
Fetches historical consumption data from Energinet API in batches
Saves locally then copies to HDFS to avoid memory issues
"""

import os
import argparse
import requests
import subprocess
import pandas as pd
from datetime import datetime, timedelta
import time

# Configuration
ENERGINET_API_BASE = "https://api.energidataservice.dk/dataset"
# Use shared volume for persistence so Replay Job can use it
DATA_DIR = "/data/historical_energy_batches"
HDFS_TARGET = "hdfs://namenode:9000/user/hive/warehouse/energy_actual"

# Fetch settings
MAX_RETRIES = 3
BATCH_SIZE = 50000  # Energinet API might have lower limits


def copy_to_hdfs(local_file, hdfs_path):
    """Copy local file to HDFS"""
    try:
        # Create HDFS directory if it doesn't exist
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', os.path.dirname(hdfs_path)],
                      check=False, capture_output=True)

        # Copy file to HDFS
        result = subprocess.run(['hdfs', 'dfs', '-put', '-f', local_file, hdfs_path],
                              check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Failed to copy to HDFS: {e.stderr}")
        return False


def fetch_batch(start_date, current_end, offset=0):
    """Fetch a batch of consumption data with pagination"""
    url = f"{ENERGINET_API_BASE}/ConsumptionCoverageLocationBased"

    start_str = start_date.strftime('%Y-%m-%dT%H:%M')
    end_str = current_end.strftime('%Y-%m-%dT%H:%M')

    params = {
        'start': start_str,
        'end': end_str,
        'limit': BATCH_SIZE,
        'offset': offset,
        'sort': 'HourDK DESC'  # Newest first
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=300)
            response.raise_for_status()
            data = response.json()
            records = data.get('records', [])
            return records
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  ✗ Error: {e}")
                return []

    return []


def process_batch_data(records):
    """Process raw consumption records into format suitable for HDFS"""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Parse timestamp to get year/month for partitioning
    # But keep the column itself as STRING to avoid Parquet/Spark timestamp compatibility issues
    temp_ts = pd.to_datetime(df['HourDK'])
    df['year'] = temp_ts.dt.year
    df['month'] = temp_ts.dt.month
    
    # Ensure timestamp columns are strings (ISO format)
    # This matches the Hive table definition (STRING) and avoids Parquet INT96/INT64 encoding issues
    df['HourDK'] = df['HourDK'].astype(str)
    if 'HourUTC' in df.columns:
        df['HourUTC'] = df['HourUTC'].astype(str)
    if 'Updated' in df.columns:
        df['Updated'] = df['Updated'].astype(str)

    # Keep all fields from ConsumptionCoverageLocationBased API
    required_cols = ['HourUTC', 'HourDK', 'PriceArea', 'ConnectedArea', 'ViaArea', 'SharePPM', 'ShareMWh', 'Updated', 'year', 'month']

    # Ensure all columns exist
    for col in required_cols:
        if col not in df.columns and col not in ['year', 'month']:
            df[col] = None

    return df[required_cols]


def main():
    """Main function - fetch in batches and copy to HDFS"""
    parser = argparse.ArgumentParser(description='Fetch historical energy data in batches')
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    print("⚡ Historical Energy Data Fetcher (Batch Mode)")
    print(f"   Date Range: {start_date.date()} to {end_date.date()}")
    print(f"   Batch Size: {BATCH_SIZE:,} records")
    print(f"   Source: ConsumptionCoverageLocationBased")
    print(f"   Local Storage: {DATA_DIR}")
    print(f"   HDFS Target: {HDFS_TARGET}")
    print("="*70)

    # Create local directory
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"DEBUG: Created directory {DATA_DIR}")

    batch_num = 0
    total_records = 0
    offset = 0
    current_end = end_date

    print(f"\nFetching batches (newest to oldest)...\n")

    while True:
        batch_num += 1

        print(f"[Batch {batch_num}] Offset {offset:,}...", end=" ")

        records = fetch_batch(start_date, current_end, offset=offset)

        if not records:
            print("✗ No data returned, stopping")
            break

        print(f"✓ {len(records):,} records")

        # Process into raw format
        df = process_batch_data(records)

        if df.empty:
            print(f"  ⚠ No records after processing, stopping")
            break

        # Get date range in this batch (using the temp year/month columns or re-parsing for display)
        # Since we converted to string, we can just take min/max of string for rough range
        print(f"  Date range: {df['HourDK'].min()} to {df['HourDK'].max()}")

        # Copy to HDFS directly (partitioned by year/month from the data)
        for (year, month), group in df.groupby(['year', 'month']):
            partition_file = os.path.join(DATA_DIR, f"energy_year={year}_month={month}_batch={batch_num}.parquet")
            
            print(f"DEBUG: Saving to {partition_file}...")
            print(f"DEBUG: Column types: {group.dtypes}") # Added debug print
            
            # Use version='1.0' AND disable dictionary encoding for maximum compatibility
            # AND we are now writing Strings for timestamps, which is very safe
            group.to_parquet(partition_file, engine='pyarrow', compression='snappy', index=False, version='1.0', use_dictionary=False)
            
            if os.path.exists(partition_file):
                size = os.path.getsize(partition_file)
                print(f"DEBUG: File saved. Size: {size} bytes")
            else:
                print(f"DEBUG: ERROR - File not found after saving!")

            hdfs_file = f"{HDFS_TARGET}/year={year}/month={month}/batch_{batch_num:04d}.parquet"
            if copy_to_hdfs(partition_file, hdfs_file):
                print(f"  ✓ HDFS: year={year}/month={month} ({len(group):,} records)")
            else:
                print(f"  ⚠ Failed to copy year={year}/month={month} to HDFS")

            # Keep file for Replay Job (do not remove)
            print(f"DEBUG: SKIPPING DELETION of {partition_file}")
            # os.remove(partition_file)

        total_records += len(df)

        # If we got less than batch size, we've reached the end
        if len(records) < BATCH_SIZE:
            print(f"  ℹ Batch smaller than limit, reached end of data")
            break

        # Update offset for next batch
        offset += BATCH_SIZE
        print("")

    # Summary
    print("\n" + "="*70)
    print(f"✅ Complete!")
    print(f"   Batches fetched: {batch_num}")
    print(f"   Total records: {total_records:,}")
    print(f"   Data location: {HDFS_TARGET}")
    print("")


if __name__ == "__main__":
    main()
