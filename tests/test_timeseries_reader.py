"""
Test module for the TimeseriesReader class.

This module contains unit tests for the main TimeseriesReader functionality,
following TDD principles.
"""
import pytest
from timeseries_data_reader.timeseries_reader import TimeseriesReader


def test_can_create_timeseries_reader():
    """
    Test that we can instantiate a TimeseriesReader object.
    
    This is our first test - it should fail initially because
    we haven't created the TimeseriesReader class yet.
    """
    reader = TimeseriesReader()
    assert reader is not None


def test_timeseries_reader_has_read_method():
    """
    Test that TimeseriesReader has a read method.
    
    This test ensures our reader can read data from a file.
    """
    reader = TimeseriesReader()
    assert hasattr(reader, 'read_data')
    assert callable(getattr(reader, 'read_data'))

def test_can_read_csv_file(sample_csv_file):
    """
    Test that TimeseriesReader can read data from a CSV file.
    
    This test checks if the reader correctly reads and parses
    timeseries data from a sample CSV file.
    """
    reader = TimeseriesReader(sample_csv_file)
    data = reader.read_data()
    assert data is not None
    assert len(data) == 4  # We expect 4 records in the sample CSV

def test_raises_error_for_nonexistent_file():
    """
    Test that TimeseriesReader raises an error for a nonexistent file.
    
    This test ensures that the reader handles file not found errors gracefully.
    """
    reader = TimeseriesReader(file_path="nonexistent_file.csv")
    with pytest.raises(FileNotFoundError):
        reader.read_data()


def test_can_read_json_file(sample_json_file):
    """
    Test that TimeseriesReader can read data from a JSON file.
    
    This test checks if the reader correctly reads and parses
    timeseries data from a sample JSON file.
    """
    reader = TimeseriesReader(sample_json_file)
    data = reader.read_data()  # This will internally call _read_json()
    
    assert data is not None
    assert len(data) == 4  # We expect 4 records in the sample JSON
    assert data[0]['timestamp'] == '2023-01-01T00:00:00'
    assert data[0]['sensor_id'] == 'sensor_1'

def test_can_read_crlx_file(sample_crlx_file):
    reader = TimeseriesReader(sample_crlx_file)
    data = reader.read_data()
    assert len(data) == 4
    assert data[0]['sensor_id'] == 'transm002005'
    assert data[0]['timestamp'] == '2025-11-14T00:05:36.704224Z'
    assert 'raw_data' in data[0]
    assert 'CST-2005DR' in data[0]['raw_data']