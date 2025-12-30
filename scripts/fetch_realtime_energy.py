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
from kafka.errors import KafkaError

# Configuration
ENERGINET_API_URL = "https://api.energidataservice.dk/dataset"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "energy_actual")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "600"))  # 10 minutes default

# Kafka Producer
producer = None

def fetch_production_actual(days_back=3):
    """Fetch actual production data from Energinet (3 days back due to data delay)"""
    # Use timezone-aware UTC
    end_time = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_time = end_time - timedelta(hours=24)

    # Production by municipality
    # Changed from ProductionConsumptionSettlement to ProductionMunicipalityHour
    url = f"{ENERGINET_API_URL}/ProductionMunicipalityHour"
    params = {
        'start': start_time.strftime('%Y-%m-%dT%H:00'),
        'end': end_time.strftime('%Y-%m-%dT%H:00'),
        'sort': 'HourUTC DESC',
        'limit': 10000
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', [])

        if records:
            df = pd.DataFrame(records)
            return df
        return None
    except Exception as e:
        print(f"  ✗ Error fetching production data: {e}")
        return None

def fetch_consumption_actual(days_back=3):
    """Fetch actual consumption data from Energinet (3 days back due to data delay)"""
    # Use timezone-aware UTC
    end_time = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_time = end_time - timedelta(hours=24)

    # Changed from ConsumptionDE35Hour to ConsumptionMunicipalityHour
    url = f"{ENERGINET_API_URL}/ConsumptionMunicipalityHour"
    params = {
        'start': start_time.strftime('%Y-%m-%dT%H:00'),
        'end': end_time.strftime('%Y-%m-%dT%H:00'),
        'sort': 'HourUTC DESC',
        'limit': 10000
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', [])

        if records:
            df = pd.DataFrame(records)
            return df
        return None
    except Exception as e:
        print(f"  ✗ Error fetching consumption data: {e}")
        return None

def aggregate_production_by_area(df):
    """Aggregate production data by DK area and hour"""
    if df is None or df.empty:
        return []

    # Parse timestamp and extract components
    df['HourDK'] = pd.to_datetime(df['HourDK'])
    df['year'] = df['HourDK'].dt.year
    df['month'] = df['HourDK'].dt.month
    df['day'] = df['HourDK'].dt.day
    df['hour'] = df['HourDK'].dt.hour

    # Map municipality to DK area (municipality < 400 = DK1, >= 400 = DK2)
    df['MunicipalityNo'] = pd.to_numeric(df['MunicipalityNo'], errors='coerce')
    df['dk_area'] = df['MunicipalityNo'].apply(lambda x: 'DK1' if x < 400 else 'DK2')

    # Calculate total production
    production_cols = ['SolarMWh', 'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh',
                      'OnshoreWindMWh', 'ThermalPowerMWh']

    for col in production_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['total_production_mwh'] = sum(df[col] for col in production_cols if col in df.columns)

    # Group by area and hour
    grouped = df.groupby(['dk_area', 'year', 'month', 'day', 'hour']).agg({
        'total_production_mwh': 'sum',
        'SolarMWh': 'sum',
        'OnshoreWindMWh': 'sum',
        'OffshoreWindLt100MW_MWh': 'sum',
        'OffshoreWindGe100MW_MWh': 'sum'
    }).reset_index()

    return grouped.to_dict('records')

def aggregate_consumption_by_area(df):
    """Aggregate consumption data by DK area and hour"""
    if df is None or df.empty:
        return []

    # Parse timestamp
    df['HourDK'] = pd.to_datetime(df['HourDK'])
    df['year'] = df['HourDK'].dt.year
    df['month'] = df['HourDK'].dt.month
    df['day'] = df['HourDK'].dt.day
    df['hour'] = df['HourDK'].dt.hour

    # Map municipality to DK area (municipality < 400 = DK1, >= 400 = DK2)
    df['MunicipalityNo'] = pd.to_numeric(df['MunicipalityNo'], errors='coerce')
    df['dk_area'] = df['MunicipalityNo'].apply(lambda x: 'DK1' if x < 400 else 'DK2')
    
    df['ConsumptionkWh'] = pd.to_numeric(df['ConsumptionkWh'], errors='coerce').fillna(0)
    # Convert kWh to MWh
    df['total_consumption_mwh'] = df['ConsumptionkWh'] / 1000.0

    # Group by area and hour
    grouped = df.groupby(['dk_area', 'year', 'month', 'day', 'hour']).agg({
        'total_consumption_mwh': 'sum'
    }).reset_index()

    return grouped.to_dict('records')

def combine_production_consumption(prod_records, cons_records):
    """Combine production and consumption into unified records"""
    # Convert to DataFrames for easier merging
    if not prod_records and not cons_records:
        return []

    prod_df = pd.DataFrame(prod_records) if prod_records else pd.DataFrame()
    cons_df = pd.DataFrame(cons_records) if cons_records else pd.DataFrame()

    # Merge on dk_area, year, month, day, hour
    if not prod_df.empty and not cons_df.empty:
        merged = pd.merge(
            prod_df, cons_df,
            on=['dk_area', 'year', 'month', 'day', 'hour'],
            how='outer'
        )
    elif not prod_df.empty:
        merged = prod_df
        merged['total_consumption_mwh'] = None
    elif not cons_df.empty:
        merged = cons_df
        merged['total_production_mwh'] = None
    else:
        return []

    # Fill NaN values
    merged = merged.fillna({
        'total_production_mwh': 0,
        'total_consumption_mwh': 0,
        'SolarMWh': 0,
        'OnshoreWindMWh': 0,
        'OffshoreWindLt100MW_MWh': 0,
        'OffshoreWindGe100MW_MWh': 0
    })

    # Convert to int where appropriate
    for col in ['year', 'month', 'day', 'hour']:
        merged[col] = merged[col].astype(int)

    # Add timestamp
    merged['timestamp'] = datetime.now(timezone.utc).isoformat()

    # Calculate net balance
    merged['net_balance_mwh'] = merged['total_production_mwh'] - merged['total_consumption_mwh']

    return merged.to_dict('records')

def send_to_kafka(records):
    """Send actual energy records to Kafka"""
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
        print(f"  ✓ Sent {len(records)} actual energy records to Kafka topic '{KAFKA_TOPIC}'")
        for r in records:
            print(f"    - {r['dk_area']} {r['year']}-{r['month']:02d}-{r['day']:02d} {r['hour']:02d}:00 | "
                  f"Prod: {r['total_production_mwh']:,.0f} MWh | "
                  f"Cons: {r['total_consumption_mwh']:,.0f} MWh | "
                  f"Net: {r['net_balance_mwh']:+,.0f} MWh")
        return True
    except Exception as e:
        print(f"  ✗ Error sending to Kafka: {e}")
        return False

def main():
    """Main loop"""
    print(f"⚡ Real-time Actual Energy Data Fetcher")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Fetch Interval: {FETCH_INTERVAL}s ({FETCH_INTERVAL/3600:.1f}h)")
    print(f"   Data Delay: 3 days (Energinet reporting lag)")
    print(f"{'='*70}\n")

    iteration = 0
    while True:
        iteration += 1
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")

        # Fetch production data (3 days back due to Energinet data delay)
        print("  Fetching production data from Energinet (3 days back)...")
        prod_df = fetch_production_actual(days_back=3)

        # Fetch consumption data (3 days back due to Energinet data delay)
        print("  Fetching consumption data from Energinet (3 days back)...")
        cons_df = fetch_consumption_actual(days_back=3)

        # Aggregate by DK area
        prod_records = aggregate_production_by_area(prod_df) if prod_df is not None else []
        cons_records = aggregate_consumption_by_area(cons_df) if cons_df is not None else []

        print(f"  Aggregated: {len(prod_records)} production records, {len(cons_records)} consumption records")

        # Combine production and consumption
        combined_records = combine_production_consumption(prod_records, cons_records)

        if combined_records:
            print(f"  Combined: {len(combined_records)} unified records")
            send_to_kafka(combined_records)
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
