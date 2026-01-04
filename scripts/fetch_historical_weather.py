#!/usr/bin/env python3
"""
Historical Weather Data Fetcher
Fetches historical weather data from DMI API month-by-month
Saves each month locally then copies to HDFS to avoid memory issues
"""

import os
import sys
import json
import argparse
import requests
import subprocess
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Configuration
DMI_API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
DMI_API_KEY = os.getenv("DMI_API_KEY", "b5800a05-4f0f-4584-b130-6129213728c0")
DATA_DIR = "/tmp/historical_weather_monthly"
HDFS_TARGET = "hdfs://namenode:9000/user/hive/warehouse/weather_observations_historical"

# DK1 stations (West Denmark)
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

# DK2 stations (East Denmark)
DK2_STATIONS = [
    "05499", "05505", "05510", "05529", "05537", "05545", "05575", "05735",
    "05880", "05889", "05935", "05945", "05960", "05970", "05981", "05986",
    "05994",
    "06135", "06136", "06138", "06141", "06147", "06149", "06151", "06154",
    "06156", "06159", "06168", "06169", "06170", "06174", "06180", "06181",
    "06183", "06186", "06187", "06188", "06190", "06191", "06193", "06197"
]

# Mapping stations to DK areas
STATION_TO_DK_AREA = {}
for s in DK1_STATIONS:
    STATION_TO_DK_AREA[s] = "DK1"
for s in DK2_STATIONS:
    STATION_TO_DK_AREA[s] = "DK2"

ALL_STATIONS = DK1_STATIONS + DK2_STATIONS

# Required parameters for ML
REQUIRED_PARAMS = ["wind_speed", "wind_speed_past1h_max", "wind_dir",
                  "radia_glob_past1h", "sun_last1h_glob", "cloud_cover",
                  "temp_mean_past1h", "humidity"]


def fetch_month_data(start_date, end_date):
    """Fetch all observations for all stations in a given time period"""
    params = {
        'api-key': DMI_API_KEY,
        'datetime': f'{start_date.strftime("%Y-%m-%dT%H:%M:%SZ")}/{end_date.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        'limit': 300000
    }

    print(f"  Fetching {start_date.strftime('%Y-%m')}...", end=" ")

    try:
        response = requests.get(DMI_API_URL, params=params, timeout=300)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        print(f"✓ {len(features):,} observations")
        return features
    except Exception as e:
        print(f"✗ Error: {e}")
        return []


def process_all_station_data(all_features):
    """Process observations from all stations into raw records suitable for Kafka/HDFS"""
    if not all_features:
        return pd.DataFrame()

    records = []
    for feature in all_features:
        props = feature.get('properties', {})
        station_id = props.get('stationId')
        timestamp_str = props.get('observed')
        param_id = props.get('parameterId')
        value = props.get('value')

        if not timestamp_str or not station_id or not param_id:
            continue

        # Parse timestamp
        try:
            ts = pd.to_datetime(timestamp_str, utc=True)
        except:
            continue

        records.append({
            'stationId': station_id,
            'parameterId': param_id,
            'timeObserved': timestamp_str,
            'value': float(value) if value is not None else None,
            'year': ts.year,
            'month': ts.month,
            'day': ts.day
        })

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


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


def fetch_batch(datetime_range, sort_order="observed,DESC"):
    """Fetch a batch of observations"""
    params = {
        'api-key': DMI_API_KEY,
        'datetime': datetime_range,
        'sortorder': sort_order,
        'limit': 300000
    }

    try:
        response = requests.get(DMI_API_URL, params=params, timeout=300)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        return features
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return []


def main():
    """Main function - fetch in batches of 300k and copy to HDFS"""
    parser = argparse.ArgumentParser(description='Fetch historical weather data in batches')
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    start_date = pd.to_datetime(args.start_date, utc=True)
    end_date   = pd.to_datetime(args.end_date,   utc=True)
    current_end = end_date

    print("🌤️  Historical Weather Data Fetcher (Batch Mode)")
    print(f"   Date Range: {start_date.date()} to {end_date.date()}")
    print(f"   Batch Size: 300,000 records")
    print(f"   Local Storage: {DATA_DIR}")
    print(f"   HDFS Target: {HDFS_TARGET}")
    print("="*70)

    # Create local directory
    os.makedirs(DATA_DIR, exist_ok=True)

    batch_num = 0
    total_records = 0

    print(f"\nFetching batches (newest to oldest)...\n")

    while current_end >= start_date:
        batch_num += 1
        datetime_range = f'{start_date.strftime("%Y-%m-%dT%H:%M:%SZ")}/{current_end.strftime("%Y-%m-%dT%H:%M:%SZ")}'

        print(f"[Batch {batch_num}] Fetching up to {current_end.strftime('%Y-%m-%d %H:%M')}...", end=" ")

        features = fetch_batch(datetime_range, sort_order="observed,DESC")

        if not features:
            print("✗ No data returned, stopping")
            break

        print(f"✓ {len(features):,} observations")

        # Process into raw format
        df = process_all_station_data(features)

        if df.empty:
            print(f"  ⚠ No records after processing, stopping")
            break

        # Get the oldest timestamp in this batch
        oldest_ts = pd.to_datetime(df["timeObserved"], utc=True).min()
        print(f"  Oldest record: {oldest_ts}")

        # Copy to HDFS directly (partitioned by year/month/day from the data)
        for (year, month, day), group in df.groupby(['year', 'month', 'day']):
            partition_file = os.path.join(DATA_DIR, f"temp_year={year}_month={month}_day={day}_batch={batch_num}.parquet")
            group.to_parquet(partition_file, engine='pyarrow', compression='snappy', index=False)

            hdfs_file = f"{HDFS_TARGET}/year={year}/month={month}/day={day}/batch_{batch_num:04d}.parquet"
            if copy_to_hdfs(partition_file, hdfs_file):
                print(f"  ✓ HDFS: year={year}/month={month}/day={day} ({len(group):,} records)")
            else:
                print(f"  ⚠ Failed to copy year={year}/month={month}/day={day} to HDFS")

            # Clean up temp file immediately
            os.remove(partition_file)

        total_records += len(df)

        # Update current_end to fetch older data in next batch
        # Subtract 1 second to avoid duplicate records
        current_end = oldest_ts - pd.Timedelta(seconds=1)

        # If we got less than 300k, we've reached the end
        if len(features) < 300000:
            print(f"  ℹ Batch smaller than limit, reached end of data")
            break

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
