#!/usr/bin/env python3
"""
Test script to verify how many Danish weather stations exist in range 05005-06197
This will help estimate the data volume before running the full fetch
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
API_KEY = os.getenv("DMI_API_KEY", "b5800a05-4f0f-4584-b130-6129213728c0")

# Generate all station IDs from 05005 to 06197 (continuous range)
ALL_STATIONS = []
for station_num in range(5005, 6198):  # 5005 to 6197 inclusive
    station_id = f"{station_num:05d}"  # Format as 5-digit string with leading zeros
    ALL_STATIONS.append(station_id)

print(f"Testing {len(ALL_STATIONS)} potential station IDs")
print(f"Range: {ALL_STATIONS[0]} to {ALL_STATIONS[-1]}")
print(f"Testing with last 7 days of data...\n")

# Test with recent data (last 7 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

stations_with_data = {}
total_checked = 0

for station_id in ALL_STATIONS:
    total_checked += 1

    params = {
        'api-key': API_KEY,
        'stationId': station_id,
        'datetime': f'{start_date.strftime("%Y-%m-%dT%H:%M:%SZ")}/{end_date.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        'limit': 100  # Just check if data exists
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])

            if features:
                # Get station name from first observation
                station_name = features[0].get('properties', {}).get('stationName', station_id)
                observation_count = len(features)
                stations_with_data[station_id] = {
                    'name': station_name,
                    'observations': observation_count
                }
                print(f"✓ {station_id}: {station_name} ({observation_count} obs)")

    except Exception as e:
        pass

    # Progress indicator every 50 stations
    if total_checked % 50 == 0:
        print(f"  ... checked {total_checked}/{len(ALL_STATIONS)} stations ({len(stations_with_data)} active)")

    # Rate limiting
    time.sleep(0.1)

print(f"\n" + "="*70)
print(f"RESULTS:")
print(f"="*70)
print(f"Total stations tested: {len(ALL_STATIONS)}")
print(f"Stations with data: {len(stations_with_data)}")
print(f"Coverage: {len(stations_with_data)/len(ALL_STATIONS)*100:.1f}%")
print(f"\nActive stations:")
print("-"*70)

for station_id, info in sorted(stations_with_data.items()):
    print(f"  {station_id}: {info['name']:<40} ({info['observations']} obs)")

print(f"\n💡 Estimated stations for full historical fetch: {len(stations_with_data)}")
print(f"   (Some stations may have historical data even if inactive now)")
