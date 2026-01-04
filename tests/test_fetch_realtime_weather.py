import pytest
import sys
import os
import json
from unittest.mock import patch, MagicMock

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import fetch_realtime_weather

@pytest.fixture
def mock_dmi_response():
    """Mock response from DMI API"""
    return {
        "features": [
            {
                "properties": {
                    "stationId": "06180",
                    "observed": "2023-10-27T10:00:00Z",
                    "parameterId": "temp_dry",
                    "value": 12.5
                }
            },
            {
                "properties": {
                    "stationId": "06180",
                    "observed": "2023-10-27T10:00:00Z",
                    "parameterId": "wind_speed",
                    "value": 5.2
                }
            }
        ]
    }

def test_fetch_station_data(mock_dmi_response):
    """Test fetching data for a single station"""
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_dmi_response
        
        data = fetch_realtime_weather.fetch_station_data("06180")
        
        assert len(data) == 2
        assert data[0]['properties']['stationId'] == "06180"
        mock_get.assert_called_once()

def test_process_raw_observations(mock_dmi_response):
    """Test processing of raw API response"""
    features = mock_dmi_response['features']
    records = fetch_realtime_weather.process_raw_observations(features, "06180")
    
    assert len(records) == 2
    
    # Check first record (temp)
    assert records[0]['stationId'] == "06180"
    assert records[0]['parameterId'] == "temp_dry"
    assert records[0]['value'] == 12.5
    assert records[0]['timeObserved'] == "2023-10-27T10:00:00Z"
    
    # Check second record (wind)
    assert records[1]['parameterId'] == "wind_speed"
    assert records[1]['value'] == 5.2

def test_process_raw_observations_invalid_value():
    """Test processing with invalid values"""
    features = [
        {
            "properties": {
                "stationId": "06180",
                "observed": "2023-10-27T10:00:00Z",
                "parameterId": "temp_dry",
                "value": "invalid" # Not a number
            }
        }
    ]
    
    records = fetch_realtime_weather.process_raw_observations(features, "06180")
    
    assert len(records) == 1
    assert records[0]['value'] is None

@patch('fetch_realtime_weather.Producer')
@patch('fetch_realtime_weather.SchemaRegistryClient')
@patch('fetch_realtime_weather.AvroSerializer')
def test_send_to_kafka(mock_serializer, mock_registry, mock_producer):
    """Test sending data to Kafka"""
    # Setup mocks
    mock_producer_instance = MagicMock()
    mock_producer.return_value = mock_producer_instance
    
    records = [
        {
            'stationId': '06180',
            'timeObserved': '2023-10-27T10:00:00Z',
            'parameterId': 'temp_dry',
            'value': 12.5
        }
    ]
    
    # Reset global producer
    fetch_realtime_weather.producer = None
    
    success = fetch_realtime_weather.send_to_kafka(records)
    
    assert success is True
    mock_producer_instance.produce.assert_called_once()
    mock_producer_instance.flush.assert_called_once()
