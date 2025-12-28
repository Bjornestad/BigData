#!/usr/bin/env python3
"""
Historical Energy Data Fetcher
Fetches historical production and consumption data from both Energinet databases
Combines and aggregates to DK1/DK2 hourly format for ML training
"""

import os
import requests
import pandas as pd
from datetime import datetime
import time

# Configuration
ENERGINET_API_BASE = "https://api.energidataservice.dk/dataset"
DATA_DIR = "data"
OUTPUT_PARQUET = os.path.join(DATA_DIR, "historical_energy_ml.parquet")

# Date range - last 4 years to match weather data
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime.now()

# Fetch settings
REQUESTS_DELAY = 0.5
MAX_RETRIES = 3


def fetch_production_data(start_date, end_date):
    """Fetch production data from ProductionConsumptionSettlement"""
    url = f"{ENERGINET_API_BASE}/ProductionConsumptionSettlement"

    start_str = start_date.strftime('%Y-%m-%dT%H:%M')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M')

    params = {
        'start': start_str,
        'end': end_str,
        'limit': 0,  # No limit - fetch all
        'sort': 'HourDK'
    }

    print(f"  Fetching production data...")
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=300)
            response.raise_for_status()
            data = response.json()
            records = data.get('records', [])
            print(f"  ✓ Fetched {len(records):,} production records")
            return pd.DataFrame(records)
        except Exception as e:
            print(f"  ✗ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                return None

    return None


def fetch_consumption_data(start_date, end_date):
    """Fetch consumption data from ConsumptionDE35Hour"""
    url = f"{ENERGINET_API_BASE}/ConsumptionDE35Hour"

    start_str = start_date.strftime('%Y-%m-%dT%H:%M')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M')

    params = {
        'start': start_str,
        'end': end_str,
        'limit': 0,  # No limit - fetch all
        'sort': 'HourDK'
    }

    print(f"  Fetching consumption data...")
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=300)
            response.raise_for_status()
            data = response.json()
            records = data.get('records', [])
            print(f"  ✓ Fetched {len(records):,} consumption records")
            return pd.DataFrame(records)
        except Exception as e:
            print(f"  ✗ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                return None

    return None


def process_production_data(df):
    """Process and aggregate production data by DK area and hour"""
    if df is None or df.empty:
        print("  No production data to process")
        return pd.DataFrame()

    print(f"\n  Processing production data...")

    # Parse timestamp
    df['HourDK'] = pd.to_datetime(df['HourDK'])
    df['year'] = df['HourDK'].dt.year
    df['month'] = df['HourDK'].dt.month
    df['day'] = df['HourDK'].dt.day
    df['hour'] = df['HourDK'].dt.hour

    # Map municipality to DK area (municipality < 400 = DK1, >= 400 = DK2)
    df['MunicipalityNo'] = pd.to_numeric(df['MunicipalityNo'], errors='coerce')
    df['dk_area'] = df['MunicipalityNo'].apply(lambda x: 'DK1' if x < 400 else 'DK2')

    # Production columns
    production_cols = ['SolarMWh', 'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh',
                      'OnshoreWindMWh', 'ThermalPowerMWh']

    for col in production_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Calculate total production
    df['total_production_mwh'] = sum(df[col] for col in production_cols if col in df.columns)

    # Group by area and hour
    grouped = df.groupby(['dk_area', 'year', 'month', 'day', 'hour']).agg({
        'total_production_mwh': 'sum',
        'SolarMWh': 'sum',
        'OnshoreWindMWh': 'sum',
        'OffshoreWindLt100MW_MWh': 'sum',
        'OffshoreWindGe100MW_MWh': 'sum'
    }).reset_index()

    print(f"  ✓ Aggregated to {len(grouped):,} hourly records")
    return grouped


def process_consumption_data(df):
    """Process and aggregate consumption data by DK area and hour"""
    if df is None or df.empty:
        print("  No consumption data to process")
        return pd.DataFrame()

    print(f"\n  Processing consumption data...")

    # Parse timestamp
    df['HourDK'] = pd.to_datetime(df['HourDK'])
    df['year'] = df['HourDK'].dt.year
    df['month'] = df['HourDK'].dt.month
    df['day'] = df['HourDK'].dt.day
    df['hour'] = df['HourDK'].dt.hour

    # Consumption is already by price area (DK1/DK2)
    df['dk_area'] = df['PriceArea']
    df['ConsumptionMWh'] = pd.to_numeric(df['ConsumptionMWh'], errors='coerce').fillna(0)

    # Group by area and hour
    grouped = df.groupby(['dk_area', 'year', 'month', 'day', 'hour']).agg({
        'ConsumptionMWh': 'sum'
    }).reset_index()

    grouped.rename(columns={'ConsumptionMWh': 'total_consumption_mwh'}, inplace=True)

    print(f"  ✓ Aggregated to {len(grouped):,} hourly records")
    return grouped


def combine_production_consumption(prod_df, cons_df):
    """Combine production and consumption into unified dataset"""
    print(f"\n  Combining production and consumption...")

    if prod_df.empty and cons_df.empty:
        print("  ✗ No data to combine")
        return pd.DataFrame()

    # Merge on dk_area, year, month, day, hour
    if not prod_df.empty and not cons_df.empty:
        merged = pd.merge(
            prod_df, cons_df,
            on=['dk_area', 'year', 'month', 'day', 'hour'],
            how='outer'
        )
    elif not prod_df.empty:
        merged = prod_df
        merged['total_consumption_mwh'] = 0
    else:
        merged = cons_df
        merged['total_production_mwh'] = 0
        merged['SolarMWh'] = 0
        merged['OnshoreWindMWh'] = 0
        merged['OffshoreWindLt100MW_MWh'] = 0
        merged['OffshoreWindGe100MW_MWh'] = 0

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

    # Add ML features
    merged['month_of_year'] = merged['month']
    merged['hour_of_day'] = merged['hour']

    # Calculate net balance
    merged['net_balance_mwh'] = merged['total_production_mwh'] - merged['total_consumption_mwh']

    # Create timestamp
    merged['timestamp'] = pd.to_datetime(
        merged[['year', 'month', 'day', 'hour']].rename(
            columns={'year': 'year', 'month': 'month', 'day': 'day', 'hour': 'hour'}
        )
    )

    # Sort by timestamp and area
    merged = merged.sort_values(['dk_area', 'timestamp']).reset_index(drop=True)

    print(f"  ✓ Combined to {len(merged):,} total records")
    return merged


def main():
    """Main function"""
    print("⚡ Historical Energy Data Fetcher")
    print(f"   Date Range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"   Production Source: ProductionConsumptionSettlement")
    print(f"   Consumption Source: ConsumptionDE35Hour")
    print("="*70)

    # Fetch production data
    print("\n[1/3] Fetching Production Data")
    prod_df_raw = fetch_production_data(START_DATE, END_DATE)

    # Fetch consumption data
    print("\n[2/3] Fetching Consumption Data")
    cons_df_raw = fetch_consumption_data(START_DATE, END_DATE)

    # Process and combine
    print("\n[3/3] Processing and Combining")
    prod_df = process_production_data(prod_df_raw)
    cons_df = process_consumption_data(cons_df_raw)
    combined_df = combine_production_consumption(prod_df, cons_df)

    if combined_df.empty:
        print("\n✗ No data to save")
        return

    # Save to parquet
    os.makedirs(DATA_DIR, exist_ok=True)
    combined_df.to_parquet(OUTPUT_PARQUET, engine='pyarrow', compression='snappy', index=False)
    file_size = os.path.getsize(OUTPUT_PARQUET)
    print(f"\n✓ Saved to {OUTPUT_PARQUET}")
    print(f"  File size: {file_size / 1024 / 1024:.2f} MB")

    # Summary
    print("\n=== Data Summary ===")
    print(f"Total records: {len(combined_df):,}")
    print(f"Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    print(f"DK1 records: {len(combined_df[combined_df['dk_area'] == 'DK1']):,}")
    print(f"DK2 records: {len(combined_df[combined_df['dk_area'] == 'DK2']):,}")
    print(f"\nProduction statistics:")
    print(f"  Total production: {combined_df['total_production_mwh'].sum():,.0f} MWh")
    print(f"  Mean hourly: {combined_df['total_production_mwh'].mean():,.0f} MWh")
    print(f"\nConsumption statistics:")
    print(f"  Total consumption: {combined_df['total_consumption_mwh'].sum():,.0f} MWh")
    print(f"  Mean hourly: {combined_df['total_consumption_mwh'].mean():,.0f} MWh")

    print(f"\nSample data:")
    print(combined_df.head())

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
