import os
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import time

# Load environment variables
load_dotenv()

# === CONFIG ===
API_URL = "https://api.energidataservice.dk/dataset/ProductionMunicipalityHour"
DATA_DIR = "data"
OUTPUT_PARQUET = os.path.join(DATA_DIR, "energy_production.parquet")
OUTPUT_CSV = os.path.join(DATA_DIR, "energy_production.csv")

# Fetch settings
REQUESTS_DELAY = 0.5
MAX_RETRIES = 3

# Date range - from 2021-01-01 to now
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime.now()


def fetch_energy_data(start_date, end_date, limit=100000, offset=0):
    """
    Fetch energy production data from Energy Data Service API.

    Args:
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        limit: Maximum number of records to fetch per request (0 = all records)
        offset: Offset for pagination

    Returns:
        dict with 'records' and metadata, or None on error
    """
    # Format dates for API (Danish timezone, format: yyyy-MM-ddTHH:mm)
    start_str = start_date.strftime('%Y-%m-%dT%H:%M')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M')

    params = {
        "start": start_str,
        "end": end_str,
        "limit": limit,
        "offset": offset,
        "sort": "HourDK"  # Sort by hour in Danish timezone
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(API_URL, params=params, timeout=120)

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


def fetch_all_energy_data(start_date, end_date, use_pagination=True):
    """
    Fetch all energy data with optional pagination.

    Args:
        start_date: Start date
        end_date: End date
        use_pagination: If False, fetch all data in one request (limit=0)

    Returns:
        List of all records
    """
    print(f"\n=== Fetching Energy Production Data ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")

    if not use_pagination:
        # Fetch all data in one request (limit=0 means no limit)
        print("Fetching all data in one request (limit=0)...\n")
        result = fetch_energy_data(start_date, end_date, limit=0, offset=0)

        if not result:
            print("No data returned")
            return []

        records = result.get('records', [])
        total = result.get('total', len(records))

        print(f"\nTotal records fetched: {len(records)}")
        print(f"API reports total: {total}")
        return records

    # Pagination approach
    all_records = []
    offset = 0
    limit = 100000

    print(f"Fetching in chunks of {limit} records\n")

    with tqdm(desc="Fetching energy data") as pbar:
        while True:
            pbar.set_description(f"Fetching offset {offset}")

            result = fetch_energy_data(start_date, end_date, limit=limit, offset=offset)

            if not result:
                print("No data returned, stopping")
                break

            records = result.get('records', [])

            if not records:
                print("No more records, stopping")
                break

            all_records.extend(records)
            pbar.set_postfix({"total_records": len(all_records)})
            pbar.update(len(records))

            # Check if we've fetched all available data
            total_records = result.get('total', 0)
            if len(all_records) >= total_records:
                print(f"\nFetched all {total_records} available records")
                break

            # Move to next chunk
            offset += limit
            time.sleep(REQUESTS_DELAY)

    print(f"\nTotal records fetched: {len(all_records)}")
    return all_records


def process_energy_data(records):
    """Convert records to DataFrame and clean data."""
    if not records:
        print("No records to process")
        return None

    print("\n=== Processing Data ===")
    df = pd.DataFrame(records)

    print(f"Initial records: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Convert timestamp column (usually HourUTC or HourDK)
    if 'HourUTC' in df.columns:
        df['timestamp'] = pd.to_datetime(df['HourUTC'])
    elif 'HourDK' in df.columns:
        df['timestamp'] = pd.to_datetime(df['HourDK'])
    else:
        print("Warning: No timestamp column found")
        return df

    # Convert numeric columns
    numeric_columns = ['ProductionGe100kW', 'ProductionLt100kW', 'Production']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Add derived date columns
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['weekday'] = df['timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday

    # Sort by timestamp and municipality
    if 'MunicipalityNo' in df.columns:
        df = df.sort_values(['MunicipalityNo', 'timestamp']).reset_index(drop=True)
    else:
        df = df.sort_values('timestamp').reset_index(drop=True)

    # Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates()
    if len(df) < initial_count:
        print(f"Removed {initial_count - len(df)} duplicate records")

    print(f"Final records: {len(df)}")

    return df


def save_data(df):
    """Save data to both Parquet and CSV formats."""
    if df is None or len(df) == 0:
        print("No data to save")
        return

    print("\n=== Saving Data ===")
    os.makedirs(DATA_DIR, exist_ok=True)

    # Save Parquet
    df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", compression="snappy", index=False)
    parquet_size = os.path.getsize(OUTPUT_PARQUET)
    print(f"✓ Parquet: {OUTPUT_PARQUET} ({parquet_size / 1024 / 1024:.2f} MB)")

    # Save CSV
    df.to_csv(OUTPUT_CSV, index=False)
    csv_size = os.path.getsize(OUTPUT_CSV)
    print(f"✓ CSV: {OUTPUT_CSV} ({csv_size / 1024 / 1024:.2f} MB)")

    if csv_size > 0:
        compression_ratio = csv_size / parquet_size
        print(f"Compression ratio: {compression_ratio:.2f}x")


def print_summary(df):
    """Print summary statistics of the data."""
    if df is None or len(df) == 0:
        return

    print("\n=== Data Summary ===")
    print(f"Total records: {len(df)}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    if 'MunicipalityNo' in df.columns:
        print(f"Unique municipalities: {df['MunicipalityNo'].nunique()}")
        print(f"\nTop 10 municipalities by record count:")
        print(df['MunicipalityNo'].value_counts().head(10))

    # Production statistics
    production_cols = ['ProductionGe100kW', 'ProductionLt100kW', 'Production']
    for col in production_cols:
        if col in df.columns:
            print(f"\n{col} statistics:")
            print(f"  Total: {df[col].sum():.2f}")
            print(f"  Mean: {df[col].mean():.2f}")
            print(f"  Min: {df[col].min():.2f}")
            print(f"  Max: {df[col].max():.2f}")

    # Sample data
    print(f"\nFirst 5 records:")
    print(df.head())

    print(f"\nData types:")
    print(df.dtypes)


def main():
    print("⚡ ENERGY PRODUCTION DATA FETCH")
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Source: Energy Data Service API")

    # Fetch all data (use_pagination=False means limit=0, fetch everything at once)
    records = fetch_all_energy_data(START_DATE, END_DATE, use_pagination=False)

    if not records:
        print("No data fetched!")
        return

    # Process data
    df = process_energy_data(records)

    if df is None:
        print("Failed to process data!")
        return

    # Save data
    save_data(df)

    # Print summary
    print_summary(df)

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
