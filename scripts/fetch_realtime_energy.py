#!/usr/bin/env python3
"""
Real-time Actual Energy Consumption Fetcher
Fetches actual consumption data from Energinet API (ConsumptionCoverageLocationBased)
Sends to Kafka for comparison with predictions
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration
ENERGINET_API_URL = "https://api.energidataservice.dk/dataset"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://kafka-schema-registry:8081")
KAFKA_TOPIC = os.getenv("ENERGY_KAFKA_TOPIC", "energy_actual")  # Use separate env var
FETCH_INTERVAL = int(os.getenv("ENERGY_FETCH_INTERVAL", "3600"))  # 60 minutes default

# Avro Schema for raw energy consumption data (from ConsumptionCoverageLocationBased API)
AVRO_SCHEMA = """{
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

# Global producer and serializer
producer = None
avro_serializer = None

def fetch_consumption_actual(hours_back=24):
    """Fetch actual consumption data from Energinet (real-time, no delay)"""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours_back)

    # Location-based consumption coverage (hourly by area)
    url = f"{ENERGINET_API_URL}/ConsumptionCoverageLocationBased"
    params = {
        'start': start_time.strftime('%Y-%m-%dT%H:%M'),
        'end': end_time.strftime('%Y-%m-%dT%H:%M'),
        'limit': 0,  # No limit - fetch all
        'sort': 'HourDK'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', [])

        if records:
            return records
        return []
    except Exception as e:
        print(f"  ✗ Error fetching consumption data: {e}")
        return []

def process_raw_records(records):
    """Process raw records to ensure correct types for Avro schema"""
    if not records:
        return []

    processed = []
    for record in records:
        # Ensure all fields are present and have correct types
        processed_record = {
            'HourUTC': str(record.get('HourUTC', '')),
            'HourDK': str(record.get('HourDK', '')),
            'PriceArea': str(record.get('PriceArea', '')),
            'ConnectedArea': str(record.get('ConnectedArea', '')),
            'ViaArea': str(record.get('ViaArea', '')),
            'SharePPM': int(record['SharePPM']) if record.get('SharePPM') is not None else None,
            'ShareMWh': float(record['ShareMWh']) if record.get('ShareMWh') is not None else None,
            'Updated': str(record.get('Updated', ''))
        }
        processed.append(processed_record)

    return processed

def dict_to_energy_actual(obj, _ctx):
    """Convert dictionary to Avro-compatible format"""
    return obj

def send_to_kafka(records):
    """Send actual energy records to Kafka using Avro with Schema Registry"""
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
                dict_to_energy_actual
            )

            # Initialize Kafka producer
            producer_conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'client.id': 'energy-fetcher'
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
            # Use HourDK and PriceArea as key
            key = f"{record['PriceArea']}_{record['HourDK']}"

            # Serialize the record to Avro
            serialized_value = avro_serializer(
                record,
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            )

            # Send to Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode('utf-8'),
                value=serialized_value
            )
            sent_count += 1

        producer.flush()
        print(f"  ✓ Sent {sent_count} Avro records to Kafka topic '{KAFKA_TOPIC}'")

        # Show sample records
        for r in records[:5]:
            share_mwh = r.get('ShareMWh', 0)
            share_mwh_str = f"{share_mwh:,.2f}" if share_mwh is not None else "null"
            print(f"    - {r['PriceArea']} → {r['ConnectedArea']} via {r['ViaArea']} | "
                  f"{r['HourDK']} | ShareMWh: {share_mwh_str}")
        if len(records) > 5:
            print(f"    ... and {len(records) - 5} more records")
        return True
    except Exception as e:
        print(f"  ✗ Error sending to Kafka: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main loop"""
    print(f"⚡ Real-time Actual Energy Data Fetcher")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Fetch Interval: {FETCH_INTERVAL}s ({FETCH_INTERVAL/3600:.1f}h)")
    print(f"   API: New Energinet API (real-time data)")
    print(f"{'='*70}\n")

    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")

        # Fetch consumption data (real-time, no delay)
        print("  Fetching consumption data from Energinet (real-time)...")
        raw_records = fetch_consumption_actual(hours_back=24)
        print(f"  Fetched: {len(raw_records)} raw consumption records")

        # Process raw records to match Avro schema
        processed_records = process_raw_records(raw_records)

        if processed_records:
            print(f"  Processed: {len(processed_records)} records")
            send_to_kafka(processed_records)
        else:
            print(f"  ✗ No data available")

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
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
