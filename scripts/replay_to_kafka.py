import os
import time
import json
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = os.getenv("DATA_DIR", "/data")
WEATHER_TOPIC = "weather-data"
ENERGY_TOPIC = "energy_actual"

# Wait settings
MAX_WAIT_MINUTES = 20
CHECK_INTERVAL_SECONDS = 30

def wait_for_data():
    """Wait until data files appear in the data directory."""
    print(f"Waiting for data in {DATA_DIR}...")
    start_time = time.time()

    while True:
        # Check for files
        if os.path.exists(DATA_DIR):
            files = os.listdir(DATA_DIR)
            weather_files = [f for f in files if f.startswith("dmi_weather_") and f.endswith(".parquet")]
            energy_files = [f for f in files if f.startswith("energy_data_") and f.endswith(".parquet")]

            if weather_files or energy_files:
                print(f"Found {len(weather_files)} weather files and {len(energy_files)} energy files.")
                total_files = len(weather_files) + len(energy_files)
                if total_files >= 5:
                    print("Sufficient data found. Starting replay.")
                    return True
                else:
                    print(f"Found {total_files} files. Waiting for more...")

        elapsed = (time.time() - start_time) / 60
        if elapsed > MAX_WAIT_MINUTES:
            print(f"Timeout reached ({MAX_WAIT_MINUTES} min). Starting with whatever data we have.")
            return False

        time.sleep(CHECK_INTERVAL_SECONDS)

def create_producer():
    """Create a Kafka producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def clean_record(record):
    """Replace NaN/Infinity with None for JSON compliance."""
    cleaned = {}
    for k, v in record.items():
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

def replay_weather_data(producer):
    """Replay weather data from Parquet files to Kafka."""
    print("Replaying weather data...")

    files = [f for f in os.listdir(DATA_DIR) if f.startswith("dmi_weather_") and f.endswith(".parquet")]
    files.sort()

    if not files:
        print("No weather data files found.")
        return

    for file in files:
        filepath = os.path.join(DATA_DIR, file)
        print(f"Processing {file}...")

        try:
            df = pd.read_parquet(filepath)
            records = df.to_dict(orient='records')

            # DEBUG: Print first record keys
            if records:
                print(f"DEBUG: Weather record keys: {list(records[0].keys())}")

            for record in records:
                record = clean_record(record)
                if 'timestamp' in record and isinstance(record['timestamp'], pd.Timestamp):
                    record['timestamp'] = record['timestamp'].isoformat()
                if 'date' in record:
                    record['date'] = str(record['date'])

                producer.send(WEATHER_TOPIC, record)

            producer.flush()
            print(f"Sent {len(records)} weather records from {file}")

        except Exception as e:
            print(f"Error processing {file}: {e}")

def replay_energy_data(producer):
    """Replay energy data from Parquet files to Kafka."""
    print("Replaying energy data...")

    files = [f for f in os.listdir(DATA_DIR) if f.startswith("energy_data_") and f.endswith(".parquet")]
    files.sort()

    if not files:
        print("No energy data files found.")
        return

    for file in files:
        filepath = os.path.join(DATA_DIR, file)
        print(f"Processing {file}...")

        try:
            df = pd.read_parquet(filepath)
            records = df.to_dict(orient='records')

            # DEBUG: Print first record keys
            if records:
                print(f"DEBUG: Energy record keys: {list(records[0].keys())}")

            for record in records:
                record = clean_record(record)

                # 1. Map Area: Use MunicipalityNo if dk_area is missing
                if 'dk_area' not in record:
                    if 'PriceArea' in record:
                        record['dk_area'] = record['PriceArea']
                    elif 'MunicipalityNo' in record:
                        # Map municipality to DK1/DK2
                        # Simple heuristic: < 400 is DK1, >= 400 is DK2
                        try:
                            muni = int(record['MunicipalityNo'])
                            record['dk_area'] = 'DK1' if muni < 400 else 'DK2'
                        except:
                            record['dk_area'] = 'Unknown'

                # 2. Calculate Total Production (Transformation)
                if 'total_production_mwh' not in record:
                    prod = 0.0
                    # Sum known production columns
                    for col in ['SolarMWh', 'OnshoreWindMWh', 'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh', 'ThermalPowerMWh']:
                        if col in record and record[col] is not None:
                            prod += float(record[col])
                    record['total_production_mwh'] = prod

                # 3. Map Consumption
                if 'total_consumption_mwh' not in record:
                    if 'GrossConsumptionMWh' in record:
                        record['total_consumption_mwh'] = record['GrossConsumptionMWh']
                    elif 'ConsumptionMWh' in record:
                        record['total_consumption_mwh'] = record['ConsumptionMWh']
                    elif 'Consumption' in record:
                        record['total_consumption_mwh'] = record['Consumption']
                    else:
                        record['total_consumption_mwh'] = 0.0

                if 'timestamp' in record and isinstance(record['timestamp'], pd.Timestamp):
                    record['timestamp'] = record['timestamp'].isoformat()
                if 'date' in record:
                    record['date'] = str(record['date'])

                producer.send(ENERGY_TOPIC, record)

            producer.flush()
            print(f"Sent {len(records)} energy records from {file}")

        except Exception as e:
            print(f"Error processing {file}: {e}")

def main():
    print("Starting replay job...")
    wait_for_data()
    time.sleep(10)
    producer = create_producer()
    if not producer:
        return
    replay_weather_data(producer)
    replay_energy_data(producer)
    producer.close()
    print("Replay job completed.")

if __name__ == "__main__":
    main()