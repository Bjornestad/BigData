import os
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import time

# Import configuration
from config import ALL_PARAMETERS, SYNOP_STATIONS, STATION_NAMES

# Load environment variables
load_dotenv()

# === CONFIG ===
API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
API_KEY = os.getenv("API_KEY")
DATA_DIR = "data"
OUTPUT_CSV = os.path.join(DATA_DIR, "november_2024_hourly.csv")

# Fetch settings
REQUESTS_DELAY = 0.5
MAX_RETRIES = 3

# November 2024
START_DATE = datetime(2025, 11, 1)
END_DATE = datetime(2025, 11, 30, 23, 59, 59)


def fetch_data(station_id, parameter_id, start_date, end_date, limit=300000):
    """Fetch weather data for a single station and parameter."""
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
            response = requests.get(API_URL, params=params, timeout=120)

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


def process_records(records):
    """Convert raw records to DataFrame rows."""
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


def fetch_november_data(stations, parameters):
    """Fetch all data for November 2024."""
    all_records = []
    total_combinations = len(stations) * len(parameters)

    print(f"\n=== Fetching November 2024 data ===")
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Stations: {len(stations)}")
    print(f"Parameters: {len(parameters)}")
    print(f"Total combinations: {total_combinations}\n")

    with tqdm(total=total_combinations, desc="November 2024") as pbar:
        for station_id in stations:
            for parameter_id in parameters:
                pbar.set_description(f"Fetching {station_id}/{parameter_id}")

                records = fetch_data(station_id, parameter_id, START_DATE, END_DATE)

                if records:
                    processed = process_records(records)
                    all_records.extend(processed)
                    pbar.set_postfix({"total_records": len(all_records)})

                pbar.update(1)
                time.sleep(REQUESTS_DELAY)

    print(f"\nFetched {len(all_records)} total records")
    return all_records


def filter_hourly(df):
    """Keep only the first datapoint per hour for each station/parameter combination."""
    print("\n=== Filtering to hourly data ===")
    print(f"Records before filtering: {len(df)}")

    # Add hour column (date + hour, no minutes/seconds)
    df["hour_timestamp"] = df["timestamp"].dt.floor("H")

    # Group by station, parameter, and hour, keep first record
    df_hourly = df.sort_values("timestamp").groupby(
        ["station_id", "parameter_id", "hour_timestamp"], as_index=False
    ).first()

    # Drop the temporary hour_timestamp column
    df_hourly = df_hourly.drop(columns=["hour_timestamp"])

    print(f"Records after filtering: {len(df_hourly)}")
    print(f"Removed {len(df) - len(df_hourly)} records")

    return df_hourly


def main():
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    print("🌍 NOVEMBER 2024 TEST FETCH (Hourly data, CSV output)")
    print(f"Active stations: {len(SYNOP_STATIONS)}")
    print(f"All parameters: {len(ALL_PARAMETERS)}")

    # Fetch all November data
    november_data = fetch_november_data(SYNOP_STATIONS, ALL_PARAMETERS)

    if not november_data:
        print("No data fetched!")
        return

    # Convert to DataFrame
    print("\n=== Processing Data ===")
    df = pd.DataFrame(november_data)

    # Convert datatypes
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove missing data
    initial_count = len(df)
    df = df.dropna(subset=["timestamp", "value"])
    print(f"Removed {initial_count - len(df)} records with missing data")

    # Filter to hourly
    df_hourly = filter_hourly(df)

    # Sort
    df_hourly = df_hourly.sort_values(["station_id", "parameter_id", "timestamp"]).reset_index(drop=True)

    # Add derived columns
    df_hourly["date"] = df_hourly["timestamp"].dt.date
    df_hourly["hour"] = df_hourly["timestamp"].dt.hour
    df_hourly["year"] = df_hourly["timestamp"].dt.year
    df_hourly["month"] = df_hourly["timestamp"].dt.month
    df_hourly["day"] = df_hourly["timestamp"].dt.day

    # Save as CSV
    print("\n=== Saving CSV ===")
    os.makedirs(DATA_DIR, exist_ok=True)
    df_hourly.to_csv(OUTPUT_CSV, index=False)

    file_size = os.path.getsize(OUTPUT_CSV)
    print(f"✓ Saved: {OUTPUT_CSV} ({file_size / 1024 / 1024:.2f} MB)")

    # Summary statistics
    print("\n=== Data Summary ===")
    print(f"Total observations: {len(df_hourly)}")
    print(f"Date range: {df_hourly['timestamp'].min()} to {df_hourly['timestamp'].max()}")
    print(f"Unique stations: {df_hourly['station_id'].nunique()}")
    print(f"Unique parameters: {df_hourly['parameter_id'].nunique()}")
    print(f"\nTop 10 stations by record count:")
    print(df_hourly["station_id"].value_counts().head(10))
    print(f"\nTop 10 parameters by record count:")
    print(df_hourly["parameter_id"].value_counts().head(10))

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
