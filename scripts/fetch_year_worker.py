"""
Kubernetes worker script to fetch data for a specific year.
Fetches both DMI weather data and Energy production/consumption data.
"""
import os
import sys
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import time
import calendar

# Add parent directory to path for imports to find config.py in root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import configuration
from config import ALL_PARAMETERS, SYNOP_STATIONS, STATION_NAMES, TEST_STATIONS, DEFAULT_PARAMETERS

# Load environment variables
load_dotenv()

# === CONFIG ===
DMI_API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
ENERGY_PROD_API_URL = "https://api.energidataservice.dk/dataset/ProductionMunicipalityHour"
ENERGY_CONS_API_URL = "https://api.energidataservice.dk/dataset/ConsumptionMunicipalityHour"
DMI_API_KEY = os.getenv("API_KEY")
DATA_DIR = os.getenv("DATA_DIR", "/data")

# Fetch settings
REQUESTS_DELAY = 0.5
MAX_RETRIES = 3


# ============ DMI WEATHER DATA FUNCTIONS ============

def fetch_dmi_data(station_id, parameter_id, start_date, end_date, limit=300000):
    """Fetch DMI weather data for a single station and parameter."""
    if not DMI_API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    params = {
        "stationId": station_id,
        "parameterId": parameter_id,
        "datetime": f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "api-key": DMI_API_KEY,
        "limit": limit,
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(DMI_API_URL, params=params, timeout=120)

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and "features" in data:
                    return data["features"]
                return []
            elif response.status_code == 404:
                return []
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                return []
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            return []

    return []


def process_dmi_records(records):
    """Convert DMI records to DataFrame rows."""
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


def fetch_dmi_year_data(year, stations, parameters, test_mode=False):
    """Fetch all DMI weather data for a specific year."""
    start_date = datetime(year, 1, 1)
    if year == datetime.now().year:
        end_date = datetime.now()
    else:
        end_date = datetime(year, 12, 31, 23, 59, 59)

    # If test mode is enabled, only fetch 1 day of data AND use subset of stations/params
    if test_mode:
        print(f"🧪 TEST MODE: Fetching only 1 day of data for {year}")
        print(f"🧪 TEST MODE: Using subset of {len(TEST_STATIONS)} stations and {len(DEFAULT_PARAMETERS)} parameters")
        end_date = start_date + timedelta(days=1)
        stations = TEST_STATIONS
        parameters = DEFAULT_PARAMETERS

    all_records = []
    total_combinations = len(stations) * len(parameters)

    print(f"\n=== Fetching DMI Weather Data for {year} ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Total combinations: {total_combinations}\n")

    with tqdm(total=total_combinations, desc=f"DMI {year}") as pbar:
        for station_id in stations:
            for parameter_id in parameters:
                pbar.set_description(f"DMI {year}: {station_id}/{parameter_id}")

                records = fetch_dmi_data(station_id, parameter_id, start_date, end_date)

                if records:
                    processed = process_dmi_records(records)
                    all_records.extend(processed)
                    pbar.set_postfix({"total_records": len(all_records)})

                pbar.update(1)
                time.sleep(REQUESTS_DELAY)

    print(f"Fetched {len(all_records)} DMI records for {year}")
    return all_records


def save_dmi_data(year_data, year):
    """Save DMI data for a specific year to Parquet."""
    if not year_data:
        print(f"No DMI data for {year}, skipping save")
        return None

    df = pd.DataFrame(year_data)

    # Convert datatypes
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove missing data
    initial_count = len(df)
    df = df.dropna(subset=["timestamp", "value"])
    print(f"Removed {initial_count - len(df)} records with missing data")

    # Sort
    df = df.sort_values(["station_id", "parameter_id", "timestamp"]).reset_index(drop=True)

    # Add derived columns
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day

    # Save to Parquet
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file = os.path.join(DATA_DIR, f"dmi_weather_{year}.parquet")
    df.to_parquet(output_file, engine="pyarrow", compression="snappy", index=False)

    file_size = os.path.getsize(output_file)
    print(f"✓ Saved DMI {year} data: {output_file} ({file_size / 1024 / 1024:.2f} MB)")
    print(f"  Records: {len(df)}")
    print(f"  Stations: {df['station_id'].nunique()}")
    print(f"  Parameters: {df['parameter_id'].nunique()}")

    return df


# ============ ENERGY DATA FUNCTIONS ============

def fetch_energy_data(url, start_date, end_date):
    """Fetch energy data (all records in one request)."""
    start_str = start_date.strftime('%Y-%m-%dT%H:%M')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M')

    params = {
        "start": start_str,
        "end": end_str,
        "limit": 0,  # No limit - fetch all
        "sort": "HourDK"
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=120)

            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"  Warning: Status {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        except Exception as e:
            print(f"  Error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            return None

    return None


def fetch_energy_year_data(year, test_mode=False):
    """Fetch all energy production and consumption data for a specific year."""
    start_date = datetime(year, 1, 1)
    if year == datetime.now().year:
        end_date = datetime.now()
    else:
        end_date = datetime(year, 12, 31, 23, 59, 59)

    # If test mode is enabled, only fetch 1 day of data
    if test_mode:
        print(f"🧪 TEST MODE: Fetching only 1 day of data for {year}")
        end_date = start_date + timedelta(days=1)

    print(f"\n=== Fetching Energy Data for {year} ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")

    # 1. Fetch Production
    print("Fetching Production data...")
    prod_result = fetch_energy_data(ENERGY_PROD_API_URL, start_date, end_date)
    prod_records = prod_result.get('records', []) if prod_result else []
    print(f"Fetched {len(prod_records)} production records")

    # 2. Fetch Consumption
    print("Fetching Consumption data...")
    cons_result = fetch_energy_data(ENERGY_CONS_API_URL, start_date, end_date)
    cons_records = cons_result.get('records', []) if cons_result else []
    print(f"Fetched {len(cons_records)} consumption records")

    return prod_records, cons_records


def save_energy_data(prod_records, cons_records, year):
    """Save energy data for a specific year to Parquet."""
    if not prod_records and not cons_records:
        print(f"No energy data for {year}, skipping save")
        return None

    # Process Production
    prod_df = pd.DataFrame(prod_records)
    if not prod_df.empty:
        if 'HourUTC' in prod_df.columns:
            prod_df['timestamp'] = pd.to_datetime(prod_df['HourUTC'])
        elif 'HourDK' in prod_df.columns:
            prod_df['timestamp'] = pd.to_datetime(prod_df['HourDK'])
        
        # Numeric columns
        for col in ['ProductionGe100kW', 'ProductionLt100kW', 'SolarMWh', 'OnshoreWindMWh', 'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh', 'ThermalPowerMWh']:
            if col in prod_df.columns:
                prod_df[col] = pd.to_numeric(prod_df[col], errors='coerce').fillna(0)

    # Process Consumption
    cons_df = pd.DataFrame(cons_records)
    if not cons_df.empty:
        if 'HourUTC' in cons_df.columns:
            cons_df['timestamp'] = pd.to_datetime(cons_df['HourUTC'])
        elif 'HourDK' in cons_df.columns:
            cons_df['timestamp'] = pd.to_datetime(cons_df['HourDK'])
            
        if 'ConsumptionMWh' in cons_df.columns:
            cons_df['ConsumptionMWh'] = pd.to_numeric(cons_df['ConsumptionMWh'], errors='coerce').fillna(0)

    # Merge
    print("Merging production and consumption...")
    if not prod_df.empty and not cons_df.empty:
        # Ensure MunicipalityNo is consistent type
        prod_df['MunicipalityNo'] = prod_df['MunicipalityNo'].astype(str)
        cons_df['MunicipalityNo'] = cons_df['MunicipalityNo'].astype(str)
        
        # Merge on Municipality and Timestamp
        # Note: We use outer join to keep all data
        df = pd.merge(
            prod_df, 
            cons_df[['MunicipalityNo', 'timestamp', 'ConsumptionMWh']], 
            on=['MunicipalityNo', 'timestamp'], 
            how='outer'
        )
    elif not prod_df.empty:
        df = prod_df
        df['ConsumptionMWh'] = 0
    else:
        df = cons_df
        df['ConsumptionMWh'] = df['ConsumptionMWh'] # already there
        # Add missing production cols
        for col in ['SolarMWh', 'OnshoreWindMWh', 'OffshoreWindLt100MW_MWh', 'OffshoreWindGe100MW_MWh', 'ThermalPowerMWh']:
            df[col] = 0

    # Fill NaNs
    df = df.fillna(0)

    # Add derived date columns
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day

    # Sort
    if 'MunicipalityNo' in df.columns:
        df = df.sort_values(['MunicipalityNo', 'timestamp']).reset_index(drop=True)
    else:
        df = df.sort_values('timestamp').reset_index(drop=True)

    # Remove duplicates
    df = df.drop_duplicates()

    # Save to Parquet
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file = os.path.join(DATA_DIR, f"energy_data_{year}.parquet")
    df.to_parquet(output_file, engine="pyarrow", compression="snappy", index=False)

    file_size = os.path.getsize(output_file)
    print(f"✓ Saved Energy {year} data: {output_file} ({file_size / 1024 / 1024:.2f} MB)")
    print(f"  Records: {len(df)}")
    if 'MunicipalityNo' in df.columns:
        print(f"  Municipalities: {df['MunicipalityNo'].nunique()}")

    return df


# ============ MAIN WORKER FUNCTION ============

def main():
    """Main worker function - fetches data for a specific year."""
    # Get year from environment variable or command line
    year = os.getenv("YEAR")
    if not year:
        if len(sys.argv) > 1:
            year = sys.argv[1]
        else:
            print("Error: YEAR environment variable or command line argument required")
            sys.exit(1)

    try:
        year = int(year)
    except ValueError:
        print(f"Error: Invalid year '{year}'")
        sys.exit(1)

    # Check for test mode
    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"

    print(f" KUBERNETES WORKER - YEAR {year}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Worker pod: {os.getenv('HOSTNAME', 'unknown')}")
    if test_mode:
        print(" RUNNING IN TEST MODE (1 day of data)")

    # Fetch DMI weather data
    try:
        dmi_records = fetch_dmi_year_data(year, SYNOP_STATIONS, ALL_PARAMETERS, test_mode)
        save_dmi_data(dmi_records, year)
    except Exception as e:
        print(f"Error fetching DMI data: {e}")

    # Fetch Energy data
    try:
        prod_records, cons_records = fetch_energy_year_data(year, test_mode)
        save_energy_data(prod_records, cons_records, year)
    except Exception as e:
        print(f"Error fetching energy data: {e}")

    print(f"\n Worker completed for year {year}!")


if __name__ == "__main__":
    main()