import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import time
import calendar

# Import configuration
from config import ALL_PARAMETERS, SYNOP_STATIONS, STATION_NAMES

# Load environment variables
load_dotenv()

# === CONFIG ===
API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
API_KEY = os.getenv("API_KEY")
DATA_DIR = "data"
OUTPUT_PARQUET = os.path.join(DATA_DIR, "weather_data_all.parquet")

# Fetch settings
REQUESTS_DELAY = 0.5
MAX_RETRIES = 3

# Date range to fetch
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime.now()


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


def get_last_day_of_month(year, month):
    """Get the last day of a given month."""
    return calendar.monthrange(year, month)[1]


def fetch_month_data(year, month, stations, parameters):
    """Fetch all data for a single month."""
    start_date = datetime(year, month, 1)

    # Handle current month - only fetch up to now
    if year == datetime.now().year and month == datetime.now().month:
        end_date = datetime.now()
    else:
        last_day = get_last_day_of_month(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)

    all_records = []
    total_combinations = len(stations) * len(parameters)

    print(f"\n=== Fetching {year}-{month:02d} data ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Total combinations: {total_combinations}\n")

    with tqdm(total=total_combinations, desc=f"{year}-{month:02d}") as pbar:
        for station_id in stations:
            for parameter_id in parameters:
                pbar.set_description(f"{year}-{month:02d}: {station_id}/{parameter_id}")

                records = fetch_data(station_id, parameter_id, start_date, end_date)

                if records:
                    processed = process_records(records)
                    all_records.extend(processed)
                    pbar.set_postfix({"total_records": len(all_records)})

                pbar.update(1)
                time.sleep(REQUESTS_DELAY)

    print(f"Fetched {len(all_records)} records for {year}-{month:02d}")
    return all_records


def save_monthly_data(month_data, year, month):
    """Save data for a single month."""
    if not month_data:
        print(f"No data for {year}-{month:02d}, skipping save")
        return None

    df = pd.DataFrame(month_data)

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

    # Save monthly parquet file
    os.makedirs(DATA_DIR, exist_ok=True)
    monthly_file = os.path.join(DATA_DIR, f"weather_data_{year}_{month:02d}.parquet")
    df.to_parquet(monthly_file, engine="pyarrow", compression="snappy", index=False)

    file_size = os.path.getsize(monthly_file)
    print(f"✓ Saved {year}-{month:02d} data: {monthly_file} ({file_size / 1024 / 1024:.2f} MB)")
    print(f"  Records: {len(df)}")
    print(f"  Stations: {df['station_id'].nunique()}")
    print(f"  Parameters: {df['parameter_id'].nunique()}")

    return df


def combine_monthly_files():
    """Combine all monthly parquet files into one."""
    print("\n=== Combining monthly files ===")

    monthly_files = [f for f in os.listdir(DATA_DIR) if f.startswith("weather_data_") and f.endswith(".parquet") and f != "weather_data_all.parquet"]
    monthly_files.sort()

    if not monthly_files:
        print("No monthly files found")
        return

    print(f"Found {len(monthly_files)} monthly files")

    dfs = []
    for file in monthly_files:
        filepath = os.path.join(DATA_DIR, file)
        df = pd.read_parquet(filepath)
        dfs.append(df)
        print(f"  Loaded {file}: {len(df)} records")

    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.sort_values(["station_id", "parameter_id", "timestamp"]).reset_index(drop=True)

    # Save combined file
    combined_df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)

    file_size = os.path.getsize(OUTPUT_PARQUET)
    print(f"\n✓ Combined file: {OUTPUT_PARQUET}")
    print(f"  Size: {file_size / 1024 / 1024:.2f} MB")
    print(f"  Total records: {len(combined_df)}")
    print(f"  Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    print(f"  Unique stations: {combined_df['station_id'].nunique()}")
    print(f"  Unique parameters: {combined_df['parameter_id'].nunique()}")


def main():
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    print("🌍 FULL SCALE DATA FETCH (Monthly increments)")
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Stations: {len(SYNOP_STATIONS)}")
    print(f"Parameters: {len(ALL_PARAMETERS)}")
    print(f"Total combinations per month: {len(SYNOP_STATIONS) * len(ALL_PARAMETERS)}")

    # Calculate total months to fetch
    total_months = (END_DATE.year - START_DATE.year) * 12 + (END_DATE.month - START_DATE.month) + 1
    print(f"Total months to fetch: {total_months}")

    # Fetch data month by month
    current_date = START_DATE
    while current_date <= END_DATE:
        year = current_date.year
        month = current_date.month

        month_data = fetch_month_data(year, month, SYNOP_STATIONS, ALL_PARAMETERS)
        save_monthly_data(month_data, year, month)

        # Move to next month
        if month == 12:
            current_date = datetime(year + 1, 1, 1)
        else:
            current_date = datetime(year, month + 1, 1)

    # Combine all monthly files
    combine_monthly_files()

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
