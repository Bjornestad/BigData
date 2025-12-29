#!/usr/bin/env python3
"""
Unified data fetcher for DMI weather and energy production data
Reads configuration from fetch_config.yaml
"""

import os
import sys
import yaml
import requests
import pandas as pd
from datetime import datetime
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from config import STATION_NAMES

def load_config(config_file='fetch_config.yaml'):
    """Load configuration from YAML file"""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def fetch_station_all_parameters(station_id, start_date, end_date, api_url, api_key, limit=300000, max_retries=3):
    """Fetch all parameters for a station at once"""
    params = {
        "stationId": station_id,
        "datetime": f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "api-key": api_key,
        "limit": limit,
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, params=params, timeout=120)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and "features" in data:
                    return data["features"]
                return []
            elif response.status_code == 404:
                return []
            else:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return []
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return []
    return []

def process_dmi_records(records):
    """Process DMI API response records"""
    processed_data = []
    for record in records:
        properties = record.get("properties", {})
        geometry = record.get("geometry", {})
        coordinates = geometry.get("coordinates", [None, None])
        station_id = properties.get("stationId")

        processed_record = {
            "station_id": station_id,
            "station_name": STATION_NAMES.get(station_id, "Unknown"),
            "parameter_id": properties.get("parameterId"),
            "timestamp": properties.get("observed"),
            "value": properties.get("value"),
            "longitude": coordinates[0] if len(coordinates) > 0 else None,
            "latitude": coordinates[1] if len(coordinates) > 1 else None,
        }
        processed_data.append(processed_record)
    return processed_data

def fetch_energy_data(start_date, end_date, api_url, max_retries=3):
    """Fetch energy production data"""
    params = {
        "start": start_date.strftime('%Y-%m-%dT%H:%M'),
        "end": end_date.strftime('%Y-%m-%dT%H:%M'),
        "limit": 0,
        "sort": "HourDK"
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, params=params, timeout=120)
            if response.status_code == 200:
                return response.json().get('records', [])
            else:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return []
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return []
    return []

def main():
    print("=" * 70)
    print("Unified Data Fetcher")
    print("=" * 70)
    print()

    # Load configuration
    print("Loading configuration...")
    config = load_config()

    start_date = datetime.strptime(config['date_range']['start'], '%Y-%m-%d')
    end_date = datetime.strptime(config['date_range']['end'], '%Y-%m-%d').replace(hour=23, minute=59, second=59)

    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Stations: {len(config['dmi']['stations'])}")
    print(f"Processing: hourly_only={config['processing']['hourly_only']}, format={config['processing']['format']}")
    print()

    # Create output directory
    os.makedirs(config['output']['directory'], exist_ok=True)

    # Fetch DMI weather data
    print("📊 Fetching DMI Weather Data")
    print("-" * 70)
    all_records = []

    for i, station_id in enumerate(config['dmi']['stations'], 1):
        print(f"[{i}/{len(config['dmi']['stations'])}] {station_id} ({STATION_NAMES.get(station_id, 'Unknown')})", flush=True)
        records = fetch_station_all_parameters(
            station_id,
            start_date,
            end_date,
            config['dmi']['api_url'],
            config['dmi']['api_key']
        )
        if records:
            all_records.extend(process_dmi_records(records))
        time.sleep(0.5)

    if all_records:
        df = pd.DataFrame(all_records)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["timestamp", "value"])

        # Add date components
        df["date"] = df["timestamp"].dt.date
        df["year"] = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month
        df["day"] = df["timestamp"].dt.day
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute

        # Sort
        df = df.sort_values(["station_id", "parameter_id", "timestamp"]).reset_index(drop=True)

        print(f"✓ Fetched {len(df):,} records")
        print()

        # Reshape to wide format if requested
        if config['processing']['format'] == 'wide':
            print("Reshaping to wide format...")
            df_wide = df.pivot_table(
                index=['station_id', 'station_name', 'timestamp', 'latitude', 'longitude', 'date', 'year', 'month', 'day', 'hour', 'minute'],
                columns='parameter_id',
                values='value',
                aggfunc='first'
            ).reset_index()
            df_wide.columns.name = None

            # Reorder columns
            id_cols = ['station_id', 'station_name', 'timestamp', 'date', 'year', 'month', 'day', 'hour', 'minute', 'latitude', 'longitude']
            param_cols = [col for col in df_wide.columns if col not in id_cols]
            df = df_wide[id_cols + sorted(param_cols)]

            print(f"✓ Reshaped to {len(df):,} rows × {len(df.columns)} columns")

        # Filter to hourly if requested
        if config['processing']['hourly_only']:
            print("Filtering to hourly data...")
            df = df[df['minute'] == 0].copy()
            print(f"✓ Filtered to {len(df):,} rows (hourly only)")

        # Save
        date_str = start_date.strftime('%Y%m')
        output_file = os.path.join(config['output']['directory'], f"dmi_weather_{date_str}.parquet")
        df.to_parquet(output_file, engine="pyarrow", compression=config['output']['compression'], index=False)
        print(f"✓ Saved: {output_file}")
        print()

    # Fetch energy data
    print("⚡ Fetching Energy Production Data")
    print("-" * 70)
    energy_records = fetch_energy_data(start_date, end_date, config['energy']['api_url'])

    if energy_records:
        df_energy = pd.DataFrame(energy_records)

        # Process timestamp
        if 'HourUTC' in df_energy.columns:
            df_energy['timestamp'] = pd.to_datetime(df_energy['HourUTC'])
        elif 'HourDK' in df_energy.columns:
            df_energy['timestamp'] = pd.to_datetime(df_energy['HourDK'])

        # Convert numeric columns
        for col in ['ProductionGe100kW', 'ProductionLt100kW', 'Production', 'SolarMWh', 'OnshoreWindMWh', 'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh', 'ThermalPowerMWh']:
            if col in df_energy.columns:
                df_energy[col] = pd.to_numeric(df_energy[col], errors='coerce')

        # Add date components
        df_energy['date'] = df_energy['timestamp'].dt.date
        df_energy['year'] = df_energy['timestamp'].dt.year
        df_energy['month'] = df_energy['timestamp'].dt.month
        df_energy['day'] = df_energy['timestamp'].dt.day
        df_energy['hour'] = df_energy['timestamp'].dt.hour

        # Remove duplicates
        df_energy = df_energy.drop_duplicates()

        # Sort by municipality if requested
        if config['processing']['sort_energy']:
            df_energy = df_energy.sort_values(['MunicipalityNo', 'timestamp']).reset_index(drop=True)

        print(f"✓ Fetched {len(df_energy):,} records")

        # Save
        date_str = start_date.strftime('%Y%m')
        output_file = os.path.join(config['output']['directory'], f"energy_production_{date_str}.parquet")
        df_energy.to_parquet(output_file, engine="pyarrow", compression=config['output']['compression'], index=False)
        print(f"✓ Saved: {output_file}")
        print()

    print("=" * 70)
    print("✅ Data fetching complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()
