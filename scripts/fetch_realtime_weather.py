#!/usr/bin/env python3
"""
Real-time Weather Data Fetcher for ML Pipeline
Fetches latest hourly weather data aggregated by DK area (DK1/DK2)
Matches the schema of weather_wind_solar_area_hourly table
Uses Avro with Schema Registry for Kafka messages
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration
DMI_API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
DMI_API_KEY = os.getenv("DMI_API_KEY", "b5800a05-4f0f-4584-b130-6129213728c0")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
# Updated to match Kubernetes service name
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw_avro") # Updated topic name to match sink
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "300"))  # 5 minutes default

# All weather stations - will get from Hive station_metadata
# For now, fetching from ALL stations (DMI API will return data for active ones)
# Station IDs in Denmark range from 05xxx to 06xxx
ALL_STATIONS = [
    "05005", "05009", "05015", "05031", "05035", "05042", "05065", "05070",
    "05075", "05081", "05085", "05089", "05095", "05105", "05109", "05135",
    "05140", "05150", "05160", "05165", "05169", "05185", "05199", "05202",
    "05205", "05220", "05225", "05269", "05272", "05276", "05277", "05290",
    "05296", "05300", "05305", "05320", "05329", "05343", "05345", "05350",
    "05355", "05365", "05375", "05381", "05384", "05395", "05400", "05406",
    "05408", "05435", "05440", "05450", "05455", "05469",
    "05499", "05505", "05510", "05529", "05537", "05545", "05575", "05735",
    "05880", "05889", "05935", "05945", "05960", "05970", "05981", "05986",
    "05994",
    "06018", "06019", "06023", "06030", "06031", "06032", "06034", "06041",
    "06043", "06049", "06051", "06052", "06056", "06058", "06060", "06065",
    "06068", "06069", "06070", "06071", "06072", "06073", "06074", "06079",
    "06080", "06081", "06082", "06088", "06089", "06093", "06096", "06102",
    "06104", "06108", "06109", "06110", "06111", "06116", "06118", "06119",
    "06120", "06123", "06124", "06126", "06132",
    "06135", "06136", "06138", "06141", "06147", "06149", "06151", "06154",
    "06156", "06159", "06168", "06169", "06170", "06174", "06180", "06181",
    "06183", "06186", "06187", "06188", "06190", "06191", "06193", "06197"
]

# Avro Schema for weather observations
AVRO_SCHEMA = """{
    "type": "record",
    "name": "WeatherObservation",
    "namespace": "dk.weather",
    "fields": [
        {"name": "stationId", "type": ["null", "string"], "default": null},
        {"name": "timeObserved", "type": ["null", "string"], "default": null},
        {"name": "parameterId", "type": ["null", "string"], "default": null},
        {"name": "value", "type": ["null", "double"], "default": null}
    ]
}"""

# Global producer and serializer
producer = None
avro_serializer = None

def dict_to_weather_observation(obj, ctx):
    """Convert dictionary to Avro-compatible format"""
    return obj

def fetch_station_data(station_id, minutes_back=10):
    """Fetch latest weather observations for a single station"""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=minutes_back)

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
        # Silent failure - many stations are inactive
        return []

def process_raw_observations(features, station_id):
    """Convert raw observations to records for Kafka"""
    if not features:
        return []

    records = []
    for feature in features:
        props = feature.get('properties', {})
        timestamp_str = props.get('observed')
        if not timestamp_str:
            continue

        # Convert value to float (Avro schema expects double, not string)
        value = props.get('value')
        if value is not None:
            try:
                value = float(value)
            except (ValueError, TypeError):
                value = None

        record = {
        'stationId': station_id,
        'timeObserved': timestamp_str,
        'parameterId': props.get('parameterId'),
        'value': value
        }
        records.append(record)

    return records

def delivery_report(err, msg):
    """Kafka delivery callback"""
    if err is not None:
        print(f'  ✗ Message delivery failed: {err}')

def send_to_kafka(records):
    """Send raw observation records to Kafka using Avro with Schema Registry"""
    global producer, avro_serializer

    if not records:
        return False

    if producer is None:
        try:
            # Initialize Schema Registry client
            schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)

            # Initialize Avro serializer
            avro_serializer = AvroSerializer(
                schema_registry_client,
                AVRO_SCHEMA,
                dict_to_weather_observation
            )

            # Initialize Kafka producer
            producer_conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'client.id': 'weather-fetcher'
            }
            producer = Producer(producer_conf)

            print(f"  ✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            print(f"  ✓ Connected to Schema Registry at {SCHEMA_REGISTRY_URL}")
            print(f"  ✓ Using Avro serialization")
        except Exception as e:
            print(f"  ✗ Failed to initialize Kafka/Schema Registry: {e}")
            import traceback
            traceback.print_exc()
            return False

    try:
        sent_count = 0
        for record in records:
            key = f"{record['stationId']}_{record['timeObserved']}"

            # Serialize the record to Avro
            serialized_value = avro_serializer(
                record,
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            )

            # Send to Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode('utf-8'),
                value=serialized_value,
                on_delivery=delivery_report
            )
            sent_count += 1

        producer.flush()
        print(f"  ✓ Sent {sent_count} Avro records to Kafka topic '{KAFKA_TOPIC}'")
        return True
    except Exception as e:
        print(f"  ✗ Error sending to Kafka: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main loop - fetch latest observations every 10 minutes"""
    print(f"🌤️  Real-time Weather Fetcher for ML Pipeline")
    print(f"   Total Stations: {len(ALL_STATIONS)}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Fetch Interval: {FETCH_INTERVAL}s ({FETCH_INTERVAL/60:.1f} min)")
    print(f"   Strategy: Fetch raw observations, Hive aggregates hourly")
    print(f"{'='*70}\n")

    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Batch {iteration}")

        # Fetch latest observations from all stations
        all_records = []
        active_stations = 0

        for station_id in ALL_STATIONS:
            features = fetch_station_data(station_id, minutes_back=15)

            if features:
                records = process_raw_observations(features, station_id)
                if records:
                    all_records.extend(records)
                    active_stations += 1
                    print(f"  Fetching {station_id}... ✓ {len(records)} obs")

        # Send all observations to Kafka
        if all_records:
            print(f"\n  Batch complete: {len(all_records)} observations from {active_stations} active stations")
            send_to_kafka(all_records)
        else:
            print(f"  ✗ No observations available this batch")

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
