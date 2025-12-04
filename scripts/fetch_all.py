import os
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import time

# Import configuration
from config import ALL_PARAMETERS, SYNOP_STATIONS, DEFAULT_PARAMETERS, TEST_STATIONS, STATION_NAMES

# Load environment variables
load_dotenv()

# === CONFIG ===
API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
API_KEY = os.getenv("API_KEY")
DATA_DIR = "data"
OUTPUT_PARQUET = os.path.join(DATA_DIR, "weather_data_all.parquet")
OUTPUT_CSV = os.path.join(DATA_DIR, "weather_data_all.csv")

# Fetch settings
REQUESTS_DELAY = 0.5  # Delay between API requests (seconds)
MAX_RETRIES = 3


def fetch_data(station_id, parameter_id, start_date, end_date, limit=300000):
    """
    Fetch weather data for a single station and parameter.

    Returns:
        List of observation records, or empty list on error
    """
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    params = {
        "stationId": station_id,
        "parameterId": parameter_id,
        "datetime": f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "api-key": API_KEY,
        "limit": limit,
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(API_URL, params=params, timeout=60)

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and "features" in data:
                    return data["features"]
                return []
            elif response.status_code == 404:
                # No data available for this combination
                return []
            else:
                print(f"  Warning: Status {response.status_code} for {station_id}/{parameter_id}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return []
        except Exception as e:
            print(f"  Error fetching {station_id}/{parameter_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            return []

    return []


def fetch_all_data(stations, parameters, start_date, end_date):
    """
    Fetch data from all stations and parameters.

    Returns:
        List of all observation records
    """
    all_records = []
    total_combinations = len(stations) * len(parameters)

    print(f"\nFetching data from {len(stations)} stations with {len(parameters)} parameters")
    print(f"Total combinations: {total_combinations}")
    print(f"Date range: {start_date.date()} to {end_date.date()}\n")

    with tqdm(total=total_combinations, desc="Overall progress") as pbar:
        for station_id in stations:
            for parameter_id in parameters:
                pbar.set_description(f"Fetching {station_id}/{parameter_id}")

                records = fetch_data(station_id, parameter_id, start_date, end_date)

                if records:
                    all_records.extend(records)
                    pbar.set_postfix({"records": len(all_records)})

                pbar.update(1)
                time.sleep(REQUESTS_DELAY)  # Rate limiting

    print(f"\nTotal records fetched: {len(all_records)}")
    return all_records


def process_weather_data(records):
    """
    Process and clean weather observation data.
    """
    if not records:
        print("No records to process")
        return pd.DataFrame()

    print("\n=== Processing Data ===")

    # Extract relevant fields
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

    # Create DataFrame
    df = pd.DataFrame(processed_data)

    print(f"Initial records: {len(df)}")

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Convert value to numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove records with missing critical values
    initial_count = len(df)
    df = df.dropna(subset=["timestamp", "value"])
    print(f"Removed {initial_count - len(df)} records with missing data")

    # Sort by station, parameter, and timestamp
    df = df.sort_values(["station_id", "parameter_id", "timestamp"]).reset_index(drop=True)

    # Add derived time columns
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day

    print(f"Final records: {len(df)}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Unique stations: {df['station_id'].nunique()}")
    print(f"Unique parameters: {df['parameter_id'].nunique()}")

    return df


def save_data(df):
    """
    Save DataFrame to both Parquet and CSV formats.
    """
    if df.empty:
        print("DataFrame is empty, nothing to save")
        return

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    print("\n=== Saving Data ===")

    # Save as Parquet
    df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)
    parquet_size = os.path.getsize(OUTPUT_PARQUET)
    print(f"✓ Parquet: {OUTPUT_PARQUET} ({parquet_size / 1024 / 1024:.2f} MB)")

    # Save as CSV
    df.to_csv(OUTPUT_CSV, index=False)
    csv_size = os.path.getsize(OUTPUT_CSV)
    print(f"✓ CSV: {OUTPUT_CSV} ({csv_size / 1024 / 1024:.2f} MB)")

    print(f"Compression ratio: {csv_size / parquet_size:.2f}x")


def main():
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    # === CONFIGURATION ===
    # Choose your mode:
    # 1. TEST: Small subset for testing
    # 2. DEFAULT: Common parameters, all synop stations
    # 3. ALL: All parameters, all synop stations (LARGE!)

    MODE = "ALL"  # Change to "TEST" or "ALL" as needed

    # Date range - Full scale: 2021-01-01 to now
    end_date = datetime.now()
    start_date = datetime(2021, 1, 1)  # January 1, 2021

    # Select stations and parameters based on mode
    if MODE == "TEST":
        stations = TEST_STATIONS
        parameters = ["temp_dry", "humidity", "wind_speed"]
        print("🧪 TEST MODE: 3 stations, 3 parameters")
    elif MODE == "DEFAULT":
        stations = SYNOP_STATIONS
        parameters = DEFAULT_PARAMETERS
        print("📊 DEFAULT MODE: All synop stations, common parameters")
    else:  # ALL
        stations = SYNOP_STATIONS
        parameters = ALL_PARAMETERS
        print("🌍 ALL MODE: All synop stations, all parameters (this will take a while!)")

    print(f"Fetching data for {start_date.date()} to {end_date.date()}")

    # Fetch data
    records = fetch_all_data(stations, parameters, start_date, end_date)

    if not records:
        print("No data fetched!")
        return

    # Process data
    df = process_weather_data(records)

    # Save data
    save_data(df)

    # Show summary
    print("\n=== Data Summary ===")
    print(f"Total observations: {len(df)}")
    print(f"\nBy station:")
    print(df.groupby("station_id").size().sort_values(ascending=False).head(10))
    print(f"\nBy parameter:")
    print(df.groupby("parameter_id").size().sort_values(ascending=False).head(10))

    print("\nDone! ✅")


if __name__ == "__main__":
    main()
