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
from kafka.errors import NoBrokersAvailable

# Configuration
ENERGINET_API_URL = "https://api.energidataservice.dk/dataset"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "energy_actual")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "600"))  # 10 minutes

producer = None

def fetch_realtime_energy(days_back=3):
    """
    Fetch 5-minute resolution real-time data and aggregate to hourly.
    Dataset: ElectricityProdex5MinRealtime
    Contains: Production, Consumption (derived), and Exchange by PriceArea.
    """
    end_time = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_time = end_time - timedelta(hours=24)

    url = f"{ENERGINET_API_URL}/ElectricityProdex5MinRealtime"

    params = {
        'start': start_time.strftime('%Y-%m-%dT%H:00'),
        'end': end_time.strftime('%Y-%m-%dT%H:00'),
        'sort': 'Minutes5UTC DESC',
        'limit': 10000
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', [])

        if not records:
            return None

        df = pd.DataFrame(records)

        # 1. Parse Time
        df['Minutes5DK'] = pd.to_datetime(df['Minutes5DK'])

        df['year'] = df['Minutes5DK'].dt.year
        df['month'] = df['Minutes5DK'].dt.month
        df['day'] = df['Minutes5DK'].dt.day
        df['hour'] = df['Minutes5DK'].dt.hour

        cols_to_clean = [
            'GrossConMWh', 'SolarPower', 'OnshoreWindPower',
            'OffshoreWindPower', 'ThermalPower', 'ProductionGe100MW',
            'ProductionLt100MW'
        ]
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0.0

        df['total_production_mwh'] = (
                df['SolarPower'] +
                df['OnshoreWindPower'] +
                df['OffshoreWindPower'] +
                df['ProductionGe100MW'] +
                df['ProductionLt100MW']
        )


        if 'GrossConMWh' not in df.columns:
             df['total_consumption_mwh'] = 0
        else:
             df['total_consumption_mwh'] = df['GrossConMWh']


        
        cols_exchange = [
            'ExchangeNO_DK1', 'ExchangeSE_DK1', 'ExchangeDE_DK1', 'ExchangeNL_DK1', 'ExchangeDK1_DK2',
            'ExchangeSE_DK2', 'ExchangeDE_DK2', 'ExchangeDK2_Bornholm'
        ]
        
        for col in cols_exchange:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0.0
                

        
        grouped = df.groupby(['PriceArea', 'year', 'month', 'day', 'hour']).agg({
            'total_production_mwh': 'mean',
            'total_consumption_mwh': 'mean',
            'SolarPower': 'mean',
            'OnshoreWindPower': 'mean',
            'OffshoreWindPower': 'mean',
            'ExchangeNO_DK1': 'mean',
            'ExchangeSE_DK1': 'mean',
            'ExchangeDE_DK1': 'mean',
            'ExchangeNL_DK1': 'mean',
            'ExchangeDK1_DK2': 'mean',
            'ExchangeSE_DK2': 'mean',
            'ExchangeDE_DK2': 'mean',
            'ExchangeDK2_Bornholm': 'mean'
        }).reset_index()

        grouped = grouped.rename(columns={'PriceArea': 'dk_area'})

        grouped = grouped[grouped['dk_area'].isin(['DK1', 'DK2'])]

        def calculate_consumption(row):
            if row['total_consumption_mwh'] > 0:
                return row['total_consumption_mwh']
            
            # Fallback calculation
            # Consumption = Production + NetImport
            
            net_import = 0
            if row['dk_area'] == 'DK1':
                # Positive is import to DK1
                net_import = (
                    row['ExchangeNO_DK1'] + 
                    row['ExchangeSE_DK1'] + 
                    row['ExchangeDE_DK1'] + 
                    row['ExchangeNL_DK1'] - 
                    row['ExchangeDK1_DK2']
                )
            elif row['dk_area'] == 'DK2':
                # Positive ExchangeDK1_DK2 is import to DK2
                # Positive ExchangeSE_DK2 is import to DK2
                # Positive ExchangeDE_DK2 is import to DK2
                # ExchangeDK2_Bornholm: usually ignored or small, but let's assume positive is export to Bornholm
                net_import = (
                    row['ExchangeSE_DK2'] + 
                    row['ExchangeDE_DK2'] + 
                    row['ExchangeDK1_DK2'] -
                    row['ExchangeDK2_Bornholm']
                )
            
            # Consumption = Production + NetImport
            # If NetImport is negative (NetExport), Consumption = Production - NetExport
            return row['total_production_mwh'] + net_import

        grouped['calculated_consumption'] = grouped.apply(calculate_consumption, axis=1)
        
        # Use calculated consumption if original is 0
        grouped['total_consumption_mwh'] = grouped.apply(
            lambda x: x['calculated_consumption'] if x['total_consumption_mwh'] == 0 else x['total_consumption_mwh'], 
            axis=1
        )
        
        # Ensure non-negative
        grouped['total_consumption_mwh'] = grouped['total_consumption_mwh'].clip(lower=0)

        # Recalculate Net Balance
        grouped['net_balance_mwh'] = grouped['total_production_mwh'] - grouped['total_consumption_mwh']

        # 10. Add Timestamp
        grouped['timestamp'] = pd.to_datetime(grouped[['year', 'month', 'day', 'hour']])
        grouped['timestamp'] = grouped['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        return grouped.to_dict('records')

    except Exception as e:
        print(f"  Error fetching real-time data: {e}")
        return None

def get_kafka_producer():
    global producer
    if producer is not None:
        return producer
    
    try:
        print(f"   Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print(f"   Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"   Failed to connect to Kafka: {e}")
        return None

def send_to_kafka(records):
    """Send actual energy records to Kafka"""
    global producer

    if not records:
        return False

    prod = get_kafka_producer()
    if prod is None:
        return False

    try:
        count = 0
        for record in records:
            key = f"{record['dk_area']}_{record['year']}_{record['month']}_{record['day']}_{record['hour']}"
            prod.send(KAFKA_TOPIC, key=key, value=record)
            count += 1

        prod.flush()
        print(f"   Sent {count} records to '{KAFKA_TOPIC}'")

        if records:
            r = records[0]
            print(f"    Sample: {r['dk_area']} {r['hour']}:00 | Prod: {r['total_production_mwh']:.0f} | Cons: {r['total_consumption_mwh']:.0f}")

        return True
    except Exception as e:
        print(f"   Error sending to Kafka: {e}")
        # Reset producer to force reconnection next time
        producer = None
        return False

def main():
    print(f" Real-time Actual Energy Data Fetcher (ProdEx5Min)")
    print(f"   Topic: {KAFKA_TOPIC}")

    iteration = 0
    while True:
        iteration += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iteration {iteration}")

        print("  Fetching real-time data (ElectricityProdex5MinRealtime)...")
        records = fetch_realtime_energy(days_back=0) # Changed to 0 to get current data

        success = False
        if records:
            success = send_to_kafka(records)
        else:
            print("   No data found")
            success = True

        if success:
            print(f"  Sleeping {FETCH_INTERVAL}s...")
            time.sleep(FETCH_INTERVAL)
        else:
            print("  Failed to send to Kafka. Retrying in 10s...")
            time.sleep(10)

if __name__ == "__main__":
    main()