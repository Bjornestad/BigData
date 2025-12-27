#!/usr/bin/env python3
"""Simple Kafka producer without waiting for confirmation"""

import json
from kafka import KafkaProducer

print("Connecting to Kafka...")

producer = KafkaProducer(
    bootstrap_servers=['kafka-bootstrap:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sending message...")

# Send without waiting for confirmation
producer.send('weather_raw', value={"message": "Sending Weather Data"})

print("Message sent (async), flushing...")
producer.flush()
print("Done!")
producer.close()
