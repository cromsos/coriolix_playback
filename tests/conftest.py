"""
Test configuration and fixtures for the timeseries data reader project.

This module provides common test fixtures and configuration that can be
shared across multiple test modules.
"""
import pytest
import tempfile
import os
from pathlib import Path


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_csv_content():
    """Sample CSV content for testing."""
    return """timestamp,value,sensor_id
2023-01-01T00:00:00,23.5,sensor_1
2023-01-01T00:01:00,24.1,sensor_1
2023-01-01T00:02:00,23.8,sensor_1
2023-01-01T00:03:00,24.2,sensor_1
"""


@pytest.fixture
def sample_json_content():
    """Sample JSON content for testing."""
    return """[
    {
        "timestamp": "2023-01-01T00:00:00",
        "value": 23.5,
        "sensor_id": "sensor_1"
    },
    {
        "timestamp": "2023-01-01T00:01:00",
        "value": 24.1,
        "sensor_id": "sensor_1"
    },
    {
        "timestamp": "2023-01-01T00:02:00",
        "value": 23.8,
        "sensor_id": "sensor_1"
    },
    {
        "timestamp": "2023-01-01T00:03:00",
        "value": 24.2,
        "sensor_id": "sensor_1"
    }
]"""


@pytest.fixture
def sample_crlx_content():
    """Sample CRLX content for testing - CORIOLIX actual format."""
    return """2025-11-14T00:05:36.704224Z transm002005 CST-2005DR	05072	06604	14647	00.212	532
2025-11-14T00:05:37.545576Z transm002005 CST-2005DR	05072	06604	14647	00.212	531
2025-11-14T00:05:38.386962Z transm002005 CST-2005DR	05072	06604	14647	00.212	532
2025-11-14T00:05:39.228399Z transm002005 CST-2005DR	05072	06604	14647	00.212	532
"""


@pytest.fixture
def sample_csv_file(temp_data_dir, sample_csv_content):
    """Create a temporary CSV file with sample data."""
    csv_file = temp_data_dir / "sample_data.csv"
    csv_file.write_text(sample_csv_content)
    return csv_file


@pytest.fixture
def sample_json_file(temp_data_dir, sample_json_content):
    """Create a temporary JSON file with sample data."""
    json_file = temp_data_dir / "sample_data.json"
    json_file.write_text(sample_json_content)
    return json_file


@pytest.fixture
def sample_crlx_file(temp_data_dir, sample_crlx_content):
    """Create a temporary CRLX file with sample data."""
    crlx_file = temp_data_dir / "sample_data.crlx"
    crlx_file.write_text(sample_crlx_content)
    return crlx_file