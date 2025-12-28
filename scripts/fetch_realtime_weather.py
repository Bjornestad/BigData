#!/usr/bin/env python3
"""
Real-time Weather Data Fetcher for ML Pipeline
Fetches latest hourly weather data aggregated by DK area (DK1/DK2)
Matches the schema of weather_wind_solar_area_hourly table
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
DMI_API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
DMI_API_KEY = os.getenv("DMI_API_KEY", "b5800a05-4f0f-4584-b130-6129213728c0")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_hourly_ml")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "3600"))  # 1 hour default

# All stations from Hive station_metadata table
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

# Kafka Producer
producer = None

# Parameter mapping
WIND_PARAMS = ["wind_speed", "wind_speed_past1h_max", "wind_dir"]
SOLAR_PARAMS = ["radia_glob_past1h", "sun_last1h_glob", "cloud_cover"]

def fetch_station_data(station_id, hours_back=1):
    """Fetch weather data for a single station"""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours_back)

    params = {
        'api-key': DMI_API_KEY,
        'stationId': station_id,
        'datetime': f'{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}/{end_time.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        'limit': 1000
    }

    try:
        response = requests.get(DMI_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('features', [])
    except Exception as e:
        print(f"  ✗ Error fetching {station_id}: {e}", file=sys.stderr)
        return []

def process_station_observations(features, station_id):
    """Process observations from a single station into hourly records"""
    if not features:
        return pd.DataFrame()

    records = []
    for feature in features:
        props = feature.get('properties', {})
        timestamp_str = props.get('observed')
        if not timestamp_str:
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
    """
    Aggregate all station data to DK area hourly format
    Matches schema: weather_wind_solar_area_hourly
    """
    if all_station_data.empty:
        return []

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
        }

        # Count stations contributing to each metric
        record['n_stations_wind'] = int(group_df['wind_speed'].notna().sum())
        record['n_stations_solar'] = int(group_df['radia_glob_past1h'].notna().sum())

        # Wind metrics (mean across stations) - matching historical table schema
        record['wind_speed_mean_area'] = group_df['wind_speed'].mean() if 'wind_speed' in group_df.columns else None
        record['wind_speed_max_area'] = group_df['wind_speed_past1h_max'].max() if 'wind_speed_past1h_max' in group_df.columns else None

        # Wind direction (convert to sin/cos and average) - matching historical table schema
        if 'wind_dir' in group_df.columns:
            wind_dirs = group_df['wind_dir'].dropna()
            if len(wind_dirs) > 0:
                wind_dirs_rad = np.deg2rad(wind_dirs)
                sin_avg = np.sin(wind_dirs_rad).mean()
                cos_avg = np.cos(wind_dirs_rad).mean()
                record['wind_dir_sin_area'] = float(sin_avg)
                record['wind_dir_cos_area'] = float(cos_avg)
            else:
                record['wind_dir_sin_area'] = None
                record['wind_dir_cos_area'] = None
        else:
            record['wind_dir_sin_area'] = None
            record['wind_dir_cos_area'] = None

        # Solar metrics (mean across stations) - matching historical table schema
        record['radia_glob_past1h_area'] = group_df['radia_glob_past1h'].mean() if 'radia_glob_past1h' in group_df.columns else None
        record['sun_last1h_glob_area'] = group_df['sun_last1h_glob'].mean() if 'sun_last1h_glob' in group_df.columns else None
        record['cloud_cover_mean_area'] = group_df['cloud_cover'].mean() if 'cloud_cover' in group_df.columns else None

        aggregated_records.append(record)

    return aggregated_records

def send_to_kafka(records):
    """Send aggregated records to Kafka"""
    global producer

    if not records:
        return False

    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print(f"  ✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            print(f"  ✗ Failed to connect to Kafka: {e}")
            return False

    try:
        for record in records:
            key = f"{record['dk_area']}_{record['year']}_{record['month']}_{record['day']}_{record['hour']}"
            future = producer.send(KAFKA_TOPIC, key=key, value=record)
            future.get(timeout=10)

        producer.flush()
        print(f"  ✓ Sent {len(records)} hourly records to Kafka topic '{KAFKA_TOPIC}'")
        for r in records:
            print(f"    - {r['dk_area']} {r['year']}-{r['month']:02d}-{r['day']:02d} {r['hour']:02d}:00 | "
                  f"Wind: {r.get('wind_speed_mean_area', 'N/A')} m/s | "
                  f"Solar: {r.get('radia_glob_past1h_area', 'N/A')} W/m²")
        return True
    except Exception as e:
        print(f"  ✗ Error sending to Kafka: {e}")
        return False

def main():
    """Main loop"""
    print(f"🌤️  Real-time Weather Fetcher for ML Pipeline")
    print(f"   DK1 Stations: {len(DK1_STATIONS)}")
    print(f"   DK2 Stations: {len(DK2_STATIONS)}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Fetch Interval: {FETCH_INTERVAL}s ({FETCH_INTERVAL/3600:.1f}h)")
    print(f"{'='*70}\n")

    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")

        # Fetch data from all stations
        all_station_dataframes = []

        for station_id in ALL_STATIONS:
            print(f"  Fetching {station_id} ({STATION_TO_DK_AREA[station_id]})...", end=" ")
            features = fetch_station_data(station_id, hours_back=2)

            if features:
                df = process_station_observations(features, station_id)
                if not df.empty:
                    all_station_dataframes.append(df)
                    print(f"✓ {len(features)} obs")
                else:
                    print(f"✗ No valid data")
            else:
                print(f"✗ Fetch failed")

        # Combine all station data
        if all_station_dataframes:
            combined_df = pd.concat(all_station_dataframes, ignore_index=True)
            print(f"\n  Combined: {len(combined_df)} station-hours")

            # Aggregate to DK area hourly
            hourly_records = aggregate_to_dk_area_hourly(combined_df)

            if hourly_records:
                print(f"  Aggregated: {len(hourly_records)} DK-area hours")
                send_to_kafka(hourly_records)
            else:
                print(f"  ✗ No aggregated records")
        else:
            print(f"  ✗ No station data available")

        # Wait for next interval
        print(f"\n  Sleeping for {FETCH_INTERVAL}s...\n")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down fetcher...")
        if producer:
            producer.close()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
