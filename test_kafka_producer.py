#!/usr/bin/env python3
"""Simple test to send a message to Kafka"""

import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

print("🔌 Connecting to Kafka at kafka-bootstrap:9092...")

try:
    producer = KafkaProducer(
        bootstrap_servers='kafka-bootstrap:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 8, 1)
    )
    print("✓ Connected to Kafka successfully!")

    # Send a simple test message
    test_message = {"message": "Sending Weather Data", "test": True}

    print(f"📤 Sending test message to 'weather_raw' topic...")
    future = producer.send('weather_raw', value=test_message)

    # Wait for confirmation
    result = future.get(timeout=10)
    print(f"✓ Message sent successfully!")
    print(f"   Topic: {result.topic}")
    print(f"   Partition: {result.partition}")
    print(f"   Offset: {result.offset}")

    producer.flush()
    producer.close()

    print("\n✅ Test completed successfully!")

except KafkaError as e:
    print(f"✗ Kafka error: {e}")
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
