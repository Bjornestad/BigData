#!/usr/bin/env python3
"""
Real-time Actual Energy Data Fetcher
Fetches actual production and consumption data from Energinet API
Sends to Kafka for frontend display and comparison with predictions
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
from kafka import KafkaProducer

# Configuration
ENERGINET_API_URL = "https://api.energidataservice.dk/dataset"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "energy_actual")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "3600"))  # Default 1 hour
DAYS_BACK = int(os.getenv("DAYS_BACK", "3"))

# Kafka Producer
producer = None

def get_producer():
    """Get or create Kafka producer"""
    global producer
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
            return None
    return producer

def fetch_settled_energy(days_back=3):
    """
    Fetch settled production and consumption data.
    Source: ProductionConsumptionSettlement (Contains both Prod and Cons)
    Resolution: Hourly per Price Area (DK1/DK2)
    """
    # Use timezone-aware UTC
    end_time = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_time = end_time - timedelta(hours=24)

    url = f"{ENERGINET_API_URL}/ProductionConsumptionSettlement"
    
    # We fetch both Production and Consumption columns
    # Note: MunicipalityNo does not exist in this dataset, which caused your 400 error.
    params = {
        'start': start_time.strftime('%Y-%m-%dT%H:00'),
        'end': end_time.strftime('%Y-%m-%dT%H:00'),
        'sort': 'HourUTC DESC',
        'limit': 1000
    }

    print(f"  Fetching data for range: {params['start']} to {params['end']}")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', [])

        if not records:
            return []

        df = pd.DataFrame(records)
        return process_energy_data(df)
        
    except requests.exceptions.HTTPError as e:
        print(f"  ✗ HTTP Error fetching data: {e}")
        if e.response is not None:
             print(f"    Response: {e.response.text}")
        return []
    except Exception as e:
        print(f"  ✗ Error fetching data: {e}")
        return []

def process_energy_data(df):
    """Process and format the Energinet dataframe for Kafka"""
    if df.empty:
        return []

    # 1. Parse timestamps
    df['HourDK'] = pd.to_datetime(df['HourDK'])
    df['year'] = df['HourDK'].dt.year
    df['month'] = df['HourDK'].dt.month
    df['day'] = df['HourDK'].dt.day
    df['hour'] = df['HourDK'].dt.hour
    
    # 2. Rename/Map Area
    # The dataset uses 'PriceArea' (DK1/DK2) natively
    df['dk_area'] = df['PriceArea']

    # 3. Ensure numeric columns
    cols_to_numeric = [
        'GrossConsumptionMWh', 'SolarMWh', 
        'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh', 
        'OnshoreWindMWh', 'ThermalPowerMWh'
    ]
    for col in cols_to_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0.0

    # 4. Calculate Aggregates
    # Production Sum
    df['total_production_mwh'] = (
        df['SolarMWh'] + 
        df['OffshoreWindLt100MW_MWh'] + 
        df['OffshoreWindGe100MW_MWh'] + 
        df['OnshoreWindMWh'] + 
        df['ThermalPowerMWh']
    )
    
    # Consumption
    df['total_consumption_mwh'] = df['GrossConsumptionMWh']
    
    # Net Balance
    df['net_balance_mwh'] = df['total_production_mwh'] - df['total_consumption_mwh']

    # 5. Timestamp for the record generation
    # Use the actual data timestamp, not the fetch time
    df['timestamp'] = df['HourDK'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    # 6. Select final columns
    final_cols = [
        'dk_area', 'year', 'month', 'day', 'hour', 'timestamp',
        'total_production_mwh', 'total_consumption_mwh', 'net_balance_mwh',
        'SolarMWh', 'OnshoreWindMWh'
    ]
    
    # Add offshore aggregate if needed
    df['OffshoreWindMWh'] = df['OffshoreWindLt100MW_MWh'] + df['OffshoreWindGe100MW_MWh']
    final_cols.append('OffshoreWindMWh')

    return df[final_cols].to_dict('records')

def send_to_kafka(records):
    """Send actual energy records to Kafka"""
    prod = get_producer()
    if not prod or not records:
        return False

    try:
        sent_count = 0
        for record in records:
            key = f"{record['dk_area']}_{record['year']}_{record['month']}_{record['day']}_{record['hour']}"
            prod.send(KAFKA_TOPIC, key=key, value=record)
            sent_count += 1

        prod.flush()
        print(f"  ✓ Sent {sent_count} records to '{KAFKA_TOPIC}'")
        
        # Log sample
        if records:
            r = records[0]
            print(f"    Sample: {r['dk_area']} {r['year']}-{r['month']:02d}-{r['day']:02d} {r['hour']:02d}:00 | "
                  f"Prod: {r['total_production_mwh']:.0f} | "
                  f"Cons: {r['total_consumption_mwh']:.0f} | "
                  f"Net: {r['net_balance_mwh']:+.0f}")
        return True
    except Exception as e:
        print(f"  ✗ Error sending to Kafka: {e}")
        # Reset producer on error to force reconnect next time
        global producer
        if producer:
            try:
                producer.close()
            except:
                pass
        producer = None
        return False

def main():
    print(f"⚡ Real-time Actual Energy Data Fetcher (Consolidated)")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Days Back: {DAYS_BACK}")
    print(f"{'='*70}\n")

    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")

        # Fetch Consolidated Data
        records = fetch_settled_energy(days_back=DAYS_BACK)

        if records:
            send_to_kafka(records)
        else:
            print(f"  ⚠ No data returned from Energinet for this period.")
            print(f"    (Note: Settlement data typically has an 8-10 day delay. Try increasing DAYS_BACK if this persists)")

        print(f"\n  Sleeping for {FETCH_INTERVAL}s...\n")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down...")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
