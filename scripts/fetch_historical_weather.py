#!/usr/bin/env python3
"""
Historical Weather Data Fetcher
Fetches historical weather data from DMI API for all stations and all required parameters
Aggregates to DK1/DK2 hourly format for ML training
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Configuration
DMI_API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
DMI_API_KEY = os.getenv("DMI_API_KEY", "b5800a05-4f0f-4584-b130-6129213728c0")
DATA_DIR = "data"
OUTPUT_PARQUET = os.path.join(DATA_DIR, "historical_weather_ml.parquet")

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


def fetch_station_data(station_id, start_date, end_date):
    """Fetch all parameters for a single station"""
    params = {
        'api-key': DMI_API_KEY,
        'stationId': station_id,
        'datetime': f'{start_date.strftime("%Y-%m-%dT%H:%M:%SZ")}/{end_date.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        'limit': 300000
    }

    try:
        response = requests.get(DMI_API_URL, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        print(f"  ✓ {station_id}: {len(features)} observations")
        return features
    except Exception as e:
        print(f"  ✗ {station_id}: {e}")
        return []


def process_all_station_data(all_features):
    """Process observations from all stations into hourly records"""
    if not all_features:
        return pd.DataFrame()

    records = []
    for feature in all_features:
        props = feature.get('properties', {})
        station_id = props.get('stationId')
        timestamp_str = props.get('observed')

        if not timestamp_str or not station_id:
            continue

        records.append({
            'station_id': station_id,
            'dk_area': STATION_TO_DK_AREA.get(station_id, 'UNKNOWN'),
            'timestamp': timestamp_str,
            'parameter_id': props.get('parameterId'),
            'value': props.get('value'),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour

    # Pivot to wide format per station
    df_wide = df.pivot_table(
        index=['station_id', 'dk_area', 'year', 'month', 'day', 'hour'],
        columns='parameter_id',
        values='value',
        aggfunc='first'
    ).reset_index()

    df_wide.columns.name = None
    return df_wide


def aggregate_to_dk_area_hourly(all_station_data):
    """Aggregate all station data to DK area hourly format for ML"""
    if all_station_data.empty:
        return pd.DataFrame()

    # Group by dk_area, year, month, day, hour
    grouped = all_station_data.groupby(['dk_area', 'year', 'month', 'day', 'hour'])

    aggregated_records = []

    for (dk_area, year, month, day, hour), group_df in grouped:
        record = {
            'dk_area': dk_area,
            'year': int(year),
            'month': int(month),
            'day': int(day),
            'hour': int(hour),
            'month_of_year': int(month),
            'hour_of_day': int(hour),
        }

        # Count stations
        record['n_stations_wind'] = int(group_df['wind_speed'].notna().sum())
        record['n_stations_solar'] = int(group_df['radia_glob_past1h'].notna().sum())

        # Wind metrics
        record['wind_speed_avg'] = group_df['wind_speed'].mean() if 'wind_speed' in group_df.columns else None
        record['wind_speed_max'] = group_df['wind_speed_past1h_max'].max() if 'wind_speed_past1h_max' in group_df.columns else None

        # Wind direction (convert to sin/cos and average)
        if 'wind_dir' in group_df.columns:
            wind_dirs = group_df['wind_dir'].dropna()
            if len(wind_dirs) > 0:
                wind_dirs_rad = np.deg2rad(wind_dirs)
                sin_avg = np.sin(wind_dirs_rad).mean()
                cos_avg = np.cos(wind_dirs_rad).mean()
                record['wind_direction_sin'] = float(sin_avg)
                record['wind_direction_cos'] = float(cos_avg)
            else:
                record['wind_direction_sin'] = None
                record['wind_direction_cos'] = None
        else:
            record['wind_direction_sin'] = None
            record['wind_direction_cos'] = None

        # Solar metrics
        record['solar_radiation_1h'] = group_df['radia_glob_past1h'].mean() if 'radia_glob_past1h' in group_df.columns else None
        record['sunshine_duration_1h'] = group_df['sun_last1h_glob'].mean() if 'sun_last1h_glob' in group_df.columns else None
        record['cloud_cover_avg'] = group_df['cloud_cover'].mean() if 'cloud_cover' in group_df.columns else None

        # Temperature and humidity
        record['temperature_avg'] = group_df['temp_mean_past1h'].mean() if 'temp_mean_past1h' in group_df.columns else None
        record['humidity_avg'] = group_df['humidity'].mean() if 'humidity' in group_df.columns else None

        # Create timestamp
        try:
            ts = datetime(year, month, day, hour, 0, 0)
            record['timestamp'] = ts.isoformat()
        except:
            record['timestamp'] = None

        aggregated_records.append(record)

    return pd.DataFrame(aggregated_records)


def main():
    """Main function"""
    print("🌤️  Historical Weather Data Fetcher")
    print(f"   DK1 Stations: {len(DK1_STATIONS)}")
    print(f"   DK2 Stations: {len(DK2_STATIONS)}")
    print(f"   Total Stations: {len(ALL_STATIONS)}")
    print("="*70)

    # Date range - last 4 years
    end_date = datetime.now()
    start_date = datetime(2021, 1, 1)

    print(f"\nDate Range: {start_date.date()} to {end_date.date()}")
    print(f"\nFetching data from {len(ALL_STATIONS)} stations...")

    all_features = []
    for i, station_id in enumerate(ALL_STATIONS, 1):
        print(f"[{i}/{len(ALL_STATIONS)}] Fetching {station_id} ({STATION_TO_DK_AREA[station_id]})...", end=" ")
        features = fetch_station_data(station_id, start_date, end_date)
        all_features.extend(features)

    print(f"\n\nTotal observations fetched: {len(all_features):,}")

    # Process data
    print("\nProcessing station data...")
    df_stations = process_all_station_data(all_features)
    print(f"  Processed to {len(df_stations):,} station-hour records")

    # Aggregate to DK area hourly
    print("\nAggregating to DK area hourly...")
    df_aggregated = aggregate_to_dk_area_hourly(df_stations)
    print(f"  Aggregated to {len(df_aggregated):,} DK-area-hour records")

    # Save to parquet
    os.makedirs(DATA_DIR, exist_ok=True)
    df_aggregated.to_parquet(OUTPUT_PARQUET, engine='pyarrow', compression='snappy', index=False)
    file_size = os.path.getsize(OUTPUT_PARQUET)
    print(f"\n✓ Saved to {OUTPUT_PARQUET}")
    print(f"  File size: {file_size / 1024 / 1024:.2f} MB")

    # Summary
    print("\n=== Data Summary ===")
    print(f"Date range: {df_aggregated['timestamp'].min()} to {df_aggregated['timestamp'].max()}")
    print(f"DK1 records: {len(df_aggregated[df_aggregated['dk_area'] == 'DK1']):,}")
    print(f"DK2 records: {len(df_aggregated[df_aggregated['dk_area'] == 'DK2']):,}")
    print(f"\nSample data:")
    print(df_aggregated.head())

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
