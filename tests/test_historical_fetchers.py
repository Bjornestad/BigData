import pytest
import pandas as pd
import sys
import os
from datetime import datetime

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import fetch_historical_energy
import fetch_historical_weather

def test_process_historical_energy_batch():
    """Test processing of historical energy records"""
    # Mock raw records from Energinet API
    raw_records = [
        {
            "HourUTC": "2023-01-01T00:00:00Z",
            "HourDK": "2023-01-01T01:00:00",
            "PriceArea": "DK1",
            "ConnectedArea": "DK2",
            "ViaArea": None,
            "SharePPM": 1000,
            "ShareMWh": 150.5,
            "Updated": "2023-01-02T10:00:00"
        }
    ]
    
    df = fetch_historical_energy.process_batch_data(raw_records)
    
    assert not df.empty
    assert "year" in df.columns
    assert "month" in df.columns
    
    # Check partition columns
    assert df.iloc[0]["year"] == 2023
    assert df.iloc[0]["month"] == 1
    
    # Check data preservation
    assert df.iloc[0]["ShareMWh"] == 150.5
    assert df.iloc[0]["PriceArea"] == "DK1"

def test_process_historical_weather_batch():
    """Test processing of historical weather observations"""
    # Mock raw features from DMI API
    raw_features = [
        {
            "properties": {
                "stationId": "06180",
                "observed": "2023-05-15T12:00:00Z",
                "parameterId": "temp_dry",
                "value": 18.5
            }
        },
        {
            "properties": {
                "stationId": "06180",
                "observed": "2023-05-15T12:00:00Z",
                "parameterId": "wind_speed",
                "value": 4.2
            }
        }
    ]
    
    df = fetch_historical_weather.process_all_station_data(raw_features)
    
    assert not df.empty
    assert len(df) == 2
    
    # Check partition columns
    assert "year" in df.columns
    assert "month" in df.columns
    assert df.iloc[0]["year"] == 2023
    assert df.iloc[0]["month"] == 5
    
    # Check values
    temp_row = df[df["parameterId"] == "temp_dry"].iloc[0]
    assert temp_row["value"] == 18.5
    assert temp_row["stationId"] == "06180"

def test_process_historical_weather_empty():
    """Test processing empty weather batch"""
    df = fetch_historical_weather.process_all_station_data([])
    assert df.empty
