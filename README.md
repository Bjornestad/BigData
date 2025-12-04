# Danish Weather & Energy Data Fetcher

Fetch and process weather data from DMI (Danish Meteorological Institute) and energy production data from the Danish Energy Data Service.

## Quick Start

### 1. Configure What to Fetch

Edit `fetch_config.yaml` to set:
- Date range (start/end dates)
- Processing options (hourly filtering, data format)

```yaml
date_range:
  start: "2025-11-01"
  end: "2025-11-30"

processing:
  hourly_only: true  # Filter to hourly data (00:00, 01:00, etc.)
  format: "wide"     # "wide" (parameters as columns) or "long"
  sort_energy: true  # Sort by municipality
```

### 2. Run the Fetcher

```bash
# Install dependencies
pip install -r requirements.txt

# Fetch data
python src/fetchers/fetch_data.py
```

### 3. View Your Data

Data is saved in the `data/` folder as Parquet files:
- `dmi_weather_YYYYMM.parquet` - Weather data
- `energy_production_YYYYMM.parquet` - Energy data

```python
import pandas as pd

# Load weather data
df_weather = pd.read_parquet('data/dmi_weather_202511.parquet')
print(df_weather.head())

# Load energy data
df_energy = pd.read_parquet('data/energy_production_202511.parquet')
print(df_energy.head())
```

## Project Structure

```
BigData/
├── fetch_config.yaml       # Configuration file (EDIT THIS)
├── config.py               # Station names and IDs
├── requirements.txt        # Python dependencies
│
├── src/
│   └── fetchers/
│       └── fetch_data.py   # Main unified fetcher
│
├── scripts/                # Old/alternative scripts
│
├── k8s/                    # Kubernetes deployment files
│
└── data/                   # Output data (gitignored)
```

## Data Sources

- **DMI API**: Danish Meteorological Institute (57 active stations, 44 parameters)
- **Energy Data Service API**: Danish electricity production (98 municipalities, hourly)

## Configuration Options

### In `fetch_config.yaml`:

- **hourly_only**: `true` = only hourly timestamps (24 rows/station/day)
- **format**:
  - `"wide"` = parameters as columns (recommended)
  - `"long"` = one row per parameter
- **sort_energy**: `true` = sort by MunicipalityNo

## Requirements

- Python 3.12+
- Libraries: requests, pandas, pyarrow, pyyaml
