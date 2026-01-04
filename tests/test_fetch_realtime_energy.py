import pytest
import sys
import os
import json
from unittest.mock import patch, MagicMock

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import fetch_realtime_energy

@pytest.fixture
def mock_energinet_response():
    """Mock response from Energinet API"""
    return {
        "records": [
            {
                "HourUTC": "2023-10-27T10:00:00Z",
                "HourDK": "2023-10-27T12:00:00",
                "PriceArea": "DK1",
                "ConnectedArea": "DK2",
                "ViaArea": "None",
                "SharePPM": 1000,
                "ShareMWh": 50.5,
                "Updated": "2023-10-27T12:15:00"
            }
        ]
    }

def test_fetch_consumption_actual(mock_energinet_response):
    """Test fetching consumption data"""
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_energinet_response
        
        records = fetch_realtime_energy.fetch_consumption_actual()
        
        assert len(records) == 1
        assert records[0]['PriceArea'] == "DK1"
        mock_get.assert_called_once()

def test_process_raw_records(mock_energinet_response):
    """Test processing of raw API records"""
    raw_records = mock_energinet_response['records']
    processed = fetch_realtime_energy.process_raw_records(raw_records)
    
    assert len(processed) == 1
    record = processed[0]
    
    assert record['HourUTC'] == "2023-10-27T10:00:00Z"
    assert record['ShareMWh'] == 50.5
    assert isinstance(record['ShareMWh'], float)
    assert isinstance(record['SharePPM'], int)

def test_process_raw_records_missing_values():
    """Test processing with missing optional values"""
    raw_records = [
        {
            "HourUTC": "2023-10-27T10:00:00Z",
            "HourDK": "2023-10-27T12:00:00",
            "PriceArea": "DK1",
            "ConnectedArea": "DK2",
            "ViaArea": "None",
            "SharePPM": None, # Missing
            "ShareMWh": None, # Missing
            "Updated": "2023-10-27T12:15:00"
        }
    ]
    
    processed = fetch_realtime_energy.process_raw_records(raw_records)
    
    assert len(processed) == 1
    record = processed[0]
    
    assert record['SharePPM'] is None
    assert record['ShareMWh'] is None

@patch('fetch_realtime_energy.Producer')
@patch('fetch_realtime_energy.SchemaRegistryClient')
@patch('fetch_realtime_energy.AvroSerializer')
def test_send_to_kafka(mock_serializer, mock_registry, mock_producer):
    """Test sending data to Kafka"""
    # Setup mocks
    mock_producer_instance = MagicMock()
    mock_producer.return_value = mock_producer_instance
    
    records = [
        {
            'HourUTC': '2023-10-27T10:00:00Z',
            'HourDK': '2023-10-27T12:00:00',
            'PriceArea': 'DK1',
            'ConnectedArea': 'DK2',
            'ViaArea': 'None',
            'SharePPM': 1000,
            'ShareMWh': 50.5,
            'Updated': '2023-10-27T12:15:00'
        }
    ]
    
    # Reset global producer
    fetch_realtime_energy.producer = None
    
    success = fetch_realtime_energy.send_to_kafka(records)
    
    assert success is True
    mock_producer_instance.produce.assert_called_once()
    mock_producer_instance.flush.assert_called_once()
