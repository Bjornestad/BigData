"""
Combine yearly Parquet files into single files.
Run this after Kubernetes jobs complete to merge all yearly data.
"""
import os
import pandas as pd
from glob import glob

DATA_DIR = os.getenv("DATA_DIR", "data")


def combine_parquet_files(pattern, output_file):
    """
    Combine multiple Parquet files matching a pattern into one file.

    Args:
        pattern: Glob pattern to match files (e.g., "dmi_weather_*.parquet")
        output_file: Output filename for combined data
    """
    # Find all matching files
    files = sorted(glob(os.path.join(DATA_DIR, pattern)))

    if not files:
        print(f"No files found matching pattern: {pattern}")
        return None

    print(f"\nCombining {len(files)} files:")
    for f in files:
        print(f"  - {os.path.basename(f)}")

    # Read and combine all files
    dfs = []
    for file in files:
        df = pd.read_parquet(file)
        dfs.append(df)
        print(f"  Loaded {os.path.basename(file)}: {len(df):,} records")

    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    # Sort by timestamp
    if 'timestamp' in combined_df.columns:
        combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
    elif 'station_id' in combined_df.columns and 'parameter_id' in combined_df.columns:
        combined_df = combined_df.sort_values(['station_id', 'parameter_id', 'timestamp']).reset_index(drop=True)

    # Remove any duplicates
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates()
    if len(combined_df) < initial_count:
        print(f"  Removed {initial_count - len(combined_df):,} duplicate records")

    # Save combined file
    output_path = os.path.join(DATA_DIR, output_file)
    combined_df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

    file_size = os.path.getsize(output_path)
    print(f"\n✓ Saved: {output_file} ({file_size / 1024 / 1024:.2f} MB)")
    print(f"  Total records: {len(combined_df):,}")
    print(f"  Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")

    return combined_df


def print_summary(df, dataset_name):
    """Print summary statistics for the dataset."""
    print(f"\n{'='*60}")
    print(f"{dataset_name} Summary")
    print(f"{'='*60}")

    if df is None:
        print("No data available")
        return

    print(f"Total records: {len(df):,}")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    if 'station_id' in df.columns:
        print(f"\nUnique stations: {df['station_id'].nunique()}")
        print("\nTop 10 stations by record count:")
        print(df['station_id'].value_counts().head(10))

    if 'parameter_id' in df.columns:
        print(f"\nUnique parameters: {df['parameter_id'].nunique()}")
        print("\nTop 10 parameters by record count:")
        print(df['parameter_id'].value_counts().head(10))

    if 'MunicipalityNo' in df.columns:
        print(f"\nUnique municipalities: {df['MunicipalityNo'].nunique()}")

    # Production statistics
    for col in ['Production', 'ProductionGe100kW', 'ProductionLt100kW']:
        if col in df.columns:
            print(f"\n{col} statistics:")
            print(f"  Total: {df[col].sum():,.2f}")
            print(f"  Mean: {df[col].mean():,.2f}")
            print(f"  Min: {df[col].min():,.2f}")
            print(f"  Max: {df[col].max():,.2f}")


def main():
    print("🔗 COMBINING YEARLY PARQUET FILES")
    print(f"Data directory: {DATA_DIR}")

    # Combine DMI weather data
    print("\n" + "="*60)
    print("Combining DMI Weather Data")
    print("="*60)
    dmi_df = combine_parquet_files("dmi_weather_*.parquet", "weather_data_all.parquet")
    if dmi_df is not None:
        print_summary(dmi_df, "DMI Weather Data")

    # Combine energy production data
    print("\n" + "="*60)
    print("Combining Energy Production Data")
    print("="*60)
    energy_df = combine_parquet_files("energy_production_*.parquet", "energy_production_all.parquet")
    if energy_df is not None:
        print_summary(energy_df, "Energy Production Data")

    print("\n✅ Done!")
    print("\nCombined files saved:")
    print(f"  - {os.path.join(DATA_DIR, 'weather_data_all.parquet')}")
    print(f"  - {os.path.join(DATA_DIR, 'energy_production_all.parquet')}")


if __name__ == "__main__":
    main()
