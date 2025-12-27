#!/usr/bin/env python3
"""
Real-time Weather Data Fetcher for Kafka Connect
Fetches latest weather data and sends to Kafka Connect REST API
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
DMI_API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
DMI_API_KEY = os.getenv("DMI_API_KEY", "b5800a05-4f0f-4584-b130-6129213728c0")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
STATION_ID = os.getenv("STATION_ID", "06080")  # Default: Esbjerg Lufthavn
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "600"))  # 10 minutes default
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")

# Initialize Kafka Producer
producer = None

# Station mapping
STATION_NAMES = {
    "06080": "Esbjerg Lufthavn",
    "06180": "København Lufthavn",
    "06070": "Århus Lufthavn",
    "06104": "Billund Lufthavn",
    "06041": "Skagen Fyr",
}

def fetch_latest_weather(station_id):
    """Fetch latest weather observation from DMI API"""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)

    params = {
        'api-key': DMI_API_KEY,
        'stationId': station_id,
        'datetime': f'{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")}/{end_time.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        'limit': 100
    }

    try:
        response = requests.get(DMI_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('features', [])
    except Exception as e:
        print(f"Error fetching data: {e}", file=sys.stderr)
        return []

def transform_to_wide_format(features, station_id):
    """Transform DMI observations to wide format"""
    if not features:
        return []

    # Collect all observations
    all_records = []
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {}).get('coordinates', [None, None])

        timestamp_str = props.get('observed')
        if not timestamp_str:
            continue

        all_records.append({
            'station_id': station_id,
            'station_name': STATION_NAMES.get(station_id, station_id),
            'parameter_id': props.get('parameterId'),
            'timestamp': timestamp_str,
            'value': props.get('value'),
            'longitude': geom[0],
            'latitude': geom[1]
        })

    if not all_records:
        return []

    # Convert to DataFrame
    df = pd.DataFrame(all_records)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute

    # Pivot to wide format
    df_wide = df.pivot_table(
        index=['station_id', 'station_name', 'latitude', 'longitude',
               'year', 'month', 'day', 'hour', 'minute'],
        columns='parameter_id',
        values='value',
        aggfunc='first'
    ).reset_index()

    df_wide.columns.name = None

    # Convert to list of dicts
    records = df_wide.to_dict('records')

    # Clean None/NaN values
    clean_records = []
    for record in records:
        clean_record = {}
        for key, value in record.items():
            if pd.notna(value):
                clean_record[key] = value
            else:
                clean_record[key] = None
        clean_records.append(clean_record)

    return clean_records

def send_to_kafka(records):
    """Send records to Kafka topic"""
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
            # Use station_id as key for partitioning
            key = record.get('station_id', 'unknown')
            future = producer.send(KAFKA_TOPIC, key=key, value=record)
            future.get(timeout=10)  # Wait for confirmation

        producer.flush()
        print(f"  ✓ Sent {len(records)} records to Kafka topic '{KAFKA_TOPIC}'")
        print(f"  Sample record: {json.dumps(records[0], indent=2, default=str)[:200]}...")
        return True
    except KafkaError as e:
        print(f"  ✗ Kafka error: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Error sending to Kafka: {e}")
        return False

def main():
    """Main loop"""
    print(f"🌤️  Starting Real-time Weather Fetcher for Kafka")
    print(f"   Station: {STATION_ID} ({STATION_NAMES.get(STATION_ID, 'Unknown')})")
    print(f"   Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Target Topic: {KAFKA_TOPIC}")
    print(f"   Fetch Interval: {FETCH_INTERVAL}s")
    print(f"{'='*60}\n")

    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")

        # Fetch latest data
        features = fetch_latest_weather(STATION_ID)

        if not features:
            print(f"  No new data available")
        else:
            # Transform to wide format
            records = transform_to_wide_format(features, STATION_ID)

            if records:
                print(f"  Fetched {len(features)} observations → {len(records)} unique timestamps")
                send_to_kafka(records)

        # Wait for next interval
        print(f"  Sleeping for {FETCH_INTERVAL}s...\n")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down fetcher...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
