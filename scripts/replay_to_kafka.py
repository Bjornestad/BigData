#!/usr/bin/env python3
"""
Replay Historical Data to Kafka
Reads historical weather and energy data from Parquet files and sends them to Kafka topics using Avro serialization.
This script is intended to be run as a Kubernetes Job to seed the system with data.
"""

import os
import time
import json
import pandas as pd
import numpy as np
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from datetime import datetime

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
DATA_DIR = os.getenv("DATA_DIR", "/data/historical") # Changed to a more specific default

# Topics
WEATHER_TOPIC = "weather_raw_avro"
ENERGY_TOPIC = "energy_actual"

# --- Avro Schemas ---
# Must match the schemas used by the consumers (Spark jobs and fetchers)
WEATHER_AVRO_SCHEMA = """{
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

ENERGY_AVRO_SCHEMA = """{
    "type": "record",
    "name": "EnergyConsumptionRaw",
    "namespace": "dk.energy",
    "fields": [
        {"name": "HourUTC", "type": "string"},
        {"name": "HourDK", "type": "string"},
        {"name": "PriceArea", "type": "string"},
        {"name": "ConnectedArea", "type": "string"},
        {"name": "ViaArea", "type": "string"},
        {"name": "SharePPM", "type": ["null", "long"], "default": null},
        {"name": "ShareMWh", "type": ["null", "double"], "default": null},
        {"name": "Updated", "type": "string"}
    ]
}"""


def create_avro_producer_and_serializers():
    """Create a Kafka producer and Avro serializers for weather and energy."""
    try:
        producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
        producer = Producer(producer_conf)

        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        weather_serializer = AvroSerializer(
            schema_registry_client,
            WEATHER_AVRO_SCHEMA,
            lambda obj, ctx: obj  # Simple dict-to-object
        )
        energy_serializer = AvroSerializer(
            schema_registry_client,
            ENERGY_AVRO_SCHEMA,
            lambda obj, ctx: obj  # Simple dict-to-object
        )

        print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"✓ Connected to Schema Registry at {SCHEMA_REGISTRY_URL}")
        return producer, weather_serializer, energy_serializer
    except Exception as e:
        print(f"✗ Failed to initialize Kafka/Schema Registry: {e}")
        return None, None, None

def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        print(f"  ✗ Message delivery failed for topic {msg.topic()}: {err}")

def clean_record(record):
    """Replace NaN/Infinity with None for Avro/JSON compliance."""
    cleaned = {}
    for k, v in record.items():
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            cleaned[k] = None
        # Convert Timestamps to ISO 8601 strings
        elif isinstance(v, pd.Timestamp) or isinstance(v, datetime):
            cleaned[k] = v.isoformat()
        else:
            cleaned[k] = v
    return cleaned

def replay_data(producer, serializer, topic, file_prefix, required_columns):
    """Generic function to replay data from Parquet files to a Kafka topic."""
    print(f"\nReplaying data for topic '{topic}'...")

    try:
        files = [f for f in os.listdir(DATA_DIR) if f.startswith(file_prefix) and f.endswith(".parquet")]
        files.sort()
    except FileNotFoundError:
        print(f"  ✗ Data directory not found: {DATA_DIR}")
        return

    if not files:
        print(f"  - No '{file_prefix}*.parquet' files found in {DATA_DIR}.")
        return

    total_sent = 0
    for file in files:
        filepath = os.path.join(DATA_DIR, file)
        print(f"  Processing {file}...")

        try:
            df = pd.read_parquet(filepath)
            
            # Ensure required columns exist
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None # Add missing columns with null values

            records = df.to_dict(orient='records')
            sent_from_file = 0

            for record in records:
                # Prepare record: clean NaNs and ensure correct types
                cleaned = clean_record(record)
                
                # Use a composite key for partitioning, e.g., time + area/station
                key_fields = [str(cleaned[k]) for k in [required_columns[0], required_columns[1]] if k in cleaned and cleaned[k] is not None]
                key = "_".join(key_fields)

                serialized_value = serializer(
                    cleaned,
                    SerializationContext(topic, MessageField.VALUE)
                )

                producer.produce(
                    topic=topic,
                    key=key.encode('utf-8'),
                    value=serialized_value,
                    on_delivery=delivery_report
                )
                sent_from_file += 1

            # Poll for delivery reports
            producer.poll(0)

            total_sent += sent_from_file
            print(f"  ✓ Queued {sent_from_file} records from {file}")

        except Exception as e:
            print(f"  ✗ Error processing {file}: {e}")
            import traceback
            traceback.print_exc()

    print(f"  Flushing producer for topic '{topic}'...")
    producer.flush()
    print(f"✅ Finished replaying for topic '{topic}'. Total records sent: {total_sent}")


def main():
    print("--- Starting Kafka Replay Job ---")
    
    producer, weather_serializer, energy_serializer = create_avro_producer_and_serializers()
    if not producer:
        return

    # Replay Weather Data
    # These are the fields defined in the Avro schema
    weather_cols = ["stationId", "timeObserved", "parameterId", "value"]
    # The historical files might have different names, we assume the replay job's volume
    # contains files named appropriately or this script is adapted.
    # For now, let's assume the historical fetcher saves with a consistent prefix.
    replay_data(producer, weather_serializer, WEATHER_TOPIC, "historical_weather", weather_cols)

    # Replay Energy Data
    # These are the fields defined in the Avro schema
    energy_cols = ["HourUTC", "HourDK", "PriceArea", "ConnectedArea", "ViaArea", "SharePPM", "ShareMWh", "Updated"]
    replay_data(producer, energy_serializer, ENERGY_TOPIC, "historical_energy", energy_cols)

    producer.close()
    print("\n--- Replay Job Completed ---")

if __name__ == "__main__":
    # Add a small delay to allow services to start, if needed
    time.sleep(int(os.getenv("REPLAY_DELAY_SECONDS", "0")))
    main()
