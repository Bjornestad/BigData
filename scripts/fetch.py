import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# === CONFIG ===
API_URL = "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
API_KEY = os.getenv("API_KEY")
DATA_DIR = "data"
OUTPUT_PARQUET = os.path.join(DATA_DIR, "weather_data.parquet")
OUTPUT_CSV = os.path.join(DATA_DIR, "weather_data.csv")

def fetch_historical_data(station_id="06186", parameter_id="temp_dry",
                          start_date=None, end_date=None, limit=300000):
    """
    Fetch historical weather data from the DMI API.

    Args:
        station_id: Weather station ID
        parameter_id: Parameter to fetch (e.g., 'temp_dry')
        start_date: Start date (YYYY-MM-DD format or datetime object)
        end_date: End date (YYYY-MM-DD format or datetime object)
        limit: Maximum number of records to fetch per request

    Returns:
        List of observation records
    """
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    # Default to last 1 year if no dates specified
    if end_date is None:
        end_date = datetime.now()
    elif isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date)

    if start_date is None:
        start_date = end_date - timedelta(days=365)
    elif isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date)

    params = {
        "stationId": station_id,
        "parameterId": parameter_id,
        "datetime": f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "api-key": API_KEY,
        "limit": limit,
    }

    print(f"Fetching data from {start_date.date()} to {end_date.date()}...")
    print(f"API URL: {API_URL}")
    print(f"Datetime param: {params['datetime']}")

    response = requests.get(API_URL, params=params, timeout=60)
    print(f"Status code: {response.status_code}")

    if response.status_code != 200:
        print(f"Error response: {response.text[:500]}")

    response.raise_for_status()
    data = response.json()

    # Extract features from the GeoJSON response
    if isinstance(data, dict) and "features" in data:
        records = data["features"]
        print(f"Fetched {len(records)} records")
        return records
    else:
        print("Unexpected response format")
        print(f"Response keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
        return []


def process_weather_data(records):
    """
    Process and clean weather observation data.

    Args:
        records: List of GeoJSON feature records from the API

    Returns:
        pandas DataFrame with cleaned data
    """
    if not records:
        print("No records to process")
        return pd.DataFrame()

    # Extract relevant fields from GeoJSON structure
    processed_data = []

    for record in records:
        properties = record.get("properties", {})
        geometry = record.get("geometry", {})
        coordinates = geometry.get("coordinates", [None, None])

        processed_record = {
            "station_id": properties.get("stationId"),
            "parameter_id": properties.get("parameterId"),
            "timestamp": properties.get("observed"),
            "value": properties.get("value"),
            "longitude": coordinates[0] if len(coordinates) > 0 else None,
            "latitude": coordinates[1] if len(coordinates) > 1 else None,
        }
        processed_data.append(processed_record)

    # Create DataFrame
    df = pd.DataFrame(processed_data)

    # Data cleaning and transformation
    print("\n=== Data Processing ===")
    print(f"Initial records: {len(df)}")

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Convert value to numeric (handle any non-numeric values)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove records with missing values
    initial_count = len(df)
    df = df.dropna(subset=["timestamp", "value"])
    print(f"Removed {initial_count - len(df)} records with missing timestamp/value")

    # Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Add derived columns
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day

    print(f"Final records: {len(df)}")
    print(f"\nDate range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"\nValue statistics:")
    print(df["value"].describe())

    return df


def save_data(df):
    """
    Save DataFrame to both Parquet and CSV formats.

    Args:
        df: pandas DataFrame
    """
    if df.empty:
        print("DataFrame is empty, nothing to save")
        return

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Save as Parquet (compressed, efficient for large datasets)
    df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)
    parquet_size = os.path.getsize(OUTPUT_PARQUET)
    print(f"\n✓ Parquet data saved to {OUTPUT_PARQUET}")
    print(f"  File size: {parquet_size / 1024:.2f} KB")

    # Save as CSV (human-readable)
    df.to_csv(OUTPUT_CSV, index=False)
    csv_size = os.path.getsize(OUTPUT_CSV)
    print(f"\n✓ CSV data saved to {OUTPUT_CSV}")
    print(f"  File size: {csv_size / 1024:.2f} KB")

    print(f"\nCompression ratio: {csv_size / parquet_size:.2f}x (CSV is {csv_size / parquet_size:.2f}x larger)")


def main():
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable not set")

    # Fetch yesterday's data for testing
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # Set to start and end of yesterday
    start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

    print(f"Fetching data for yesterday: {yesterday.date()}")

    records = fetch_historical_data(
        station_id="06186",
        parameter_id="temp_dry",
        start_date=start_date,
        end_date=end_date
    )

    # Process the data
    df = process_weather_data(records)

    # Save to both Parquet and CSV
    save_data(df)

    print("\n=== Sample Data ===")
    print(df.head(10))

    print("\nDone!")


if __name__ == "__main__":
    main()
