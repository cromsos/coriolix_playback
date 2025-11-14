"""
Tests for configuration file functionality.

Tests YAML configuration parsing, validation, and multi-stream execution.
"""
import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

from timeseries_data_reader.config import ConfigParser, StreamConfig, execute_config


def test_parse_basic_config():
    """Test parsing of basic YAML configuration."""
    config_content = """
streams:
  - name: "test_stream"
    file: "test.crlx"
    protocol: "udp_broadcast"
    port: 8080
    broadcast_addr: "192.168.1.255"
    sensor_id: "sensor_001"
    update_timestamp: true
    interval: 0.5

defaults:
  update_timestamp: true
  interval: 1.0
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        parser = ConfigParser()
        config = parser.parse(config_file)
        
        assert len(config.streams) == 1
        stream = config.streams[0]
        
        assert stream.name == "test_stream"
        assert stream.file == "test.crlx"
        assert stream.protocol == "udp_broadcast"
        assert stream.port == 8080
        assert stream.broadcast_addr == "192.168.1.255"
        assert stream.sensor_id == "sensor_001"
        assert stream.update_timestamp is True
        assert stream.interval == 0.5
        
        # Test defaults
        assert config.defaults.update_timestamp is True
        assert config.defaults.interval == 1.0
        
    finally:
        Path(config_file).unlink()


def test_parse_config_with_defaults():
    """Test that stream configs inherit from defaults."""
    config_content = """
streams:
  - name: "minimal_stream"
    file: "test.crlx"
    protocol: "udp_broadcast"
    port: 8080
    broadcast_addr: "192.168.1.255"
    # sensor_id, interval, update_timestamp should come from defaults

defaults:
  sensor_id: "default_sensor"
  update_timestamp: false
  interval: 2.0
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        parser = ConfigParser()
        config = parser.parse(config_file)
        
        stream = config.streams[0]
        assert stream.sensor_id == "default_sensor"  # From defaults
        assert stream.update_timestamp is False      # From defaults
        assert stream.interval == 2.0               # From defaults
        
    finally:
        Path(config_file).unlink()


def test_parse_multiple_streams():
    """Test parsing configuration with multiple streams."""
    config_content = """
streams:
  - name: "broadcast_stream"
    file: "sensor1.crlx"
    protocol: "udp_broadcast"
    port: 8080
    broadcast_addr: "192.168.1.255"

  - name: "unicast_stream"
    file: "sensor2.crlx"
    protocol: "udp_unicast"
    port: 8081
    unicast_addr: "192.168.1.100"

  - name: "tcp_stream"
    file: "sensor3.crlx"
    protocol: "tcp"
    host: "localhost"
    port: 9090

defaults:
  update_timestamp: true
  interval: 1.0
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        parser = ConfigParser()
        config = parser.parse(config_file)
        
        assert len(config.streams) == 3
        
        # Check broadcast stream
        broadcast = config.streams[0]
        assert broadcast.protocol == "udp_broadcast"
        assert broadcast.port == 8080
        
        # Check unicast stream
        unicast = config.streams[1]
        assert unicast.protocol == "udp_unicast"
        assert unicast.unicast_addr == "192.168.1.100"
        
        # Check TCP stream
        tcp = config.streams[2]
        assert tcp.protocol == "tcp"
        assert tcp.host == "localhost"
        
    finally:
        Path(config_file).unlink()


def test_config_validation_errors():
    """Test configuration validation catches errors."""
    parser = ConfigParser()
    
    # Missing required broadcast_addr
    invalid_config = """
streams:
  - name: "invalid_stream"
    file: "test.crlx"
    protocol: "udp_broadcast"
    port: 8080
    # missing broadcast_addr
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(invalid_config)
        config_file = f.name

    try:
        with pytest.raises(ValueError, match="broadcast_addr is required"):
            parser.parse(config_file)
    finally:
        Path(config_file).unlink()


def test_execute_config_single_stream(sample_crlx_file):
    """Test execution of config with single stream."""
    
    config_content = f"""
streams:
  - name: "test_stream"
    file: "{sample_crlx_file}"
    protocol: "udp_broadcast"
    port: 8080
    broadcast_addr: "127.0.0.1"
    interval: 0.1

defaults:
  update_timestamp: true
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        with patch('timeseries_data_reader.config.TimeseriesReader') as mock_reader_class:
            mock_reader = MagicMock()
            mock_reader.stream_data.return_value = 4
            mock_reader_class.return_value = mock_reader
            
            results = execute_config(config_file)
            
            assert len(results) == 1
            assert results[0]['name'] == 'test_stream'
            assert results[0]['records_sent'] == 4
            assert results[0]['success'] is True
            
            # Verify TimeseriesReader was called correctly
            mock_reader_class.assert_called_once_with(str(sample_crlx_file))
            mock_reader.stream_data.assert_called_once_with(
                protocol='udp_broadcast',
                port=8080,
                broadcast_addr='127.0.0.1',
                sensor_id=None,
                update_timestamp=True,
                interval=0.1
            )
            
    finally:
        Path(config_file).unlink()


def test_execute_config_multiple_streams_parallel(sample_crlx_file):
    """Test parallel execution of multiple streams from config."""
    
    config_content = f"""
streams:
  - name: "stream_1"
    file: "{sample_crlx_file}"
    protocol: "udp_broadcast"
    port: 8080
    broadcast_addr: "127.0.0.1"

  - name: "stream_2"
    file: "{sample_crlx_file}"
    protocol: "udp_unicast"
    port: 8081
    unicast_addr: "127.0.0.1"

defaults:
  interval: 0.1
  update_timestamp: true
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_file = f.name

    try:
        with patch('timeseries_data_reader.config.TimeseriesReader') as mock_reader_class:
            mock_reader = MagicMock()
            mock_reader.stream_data.return_value = 4
            mock_reader_class.return_value = mock_reader
            
            results = execute_config(config_file, parallel=True)
            
            assert len(results) == 2
            assert all(result['success'] for result in results)
            assert {result['name'] for result in results} == {'stream_1', 'stream_2'}
            
            # Both streams should have been executed
            assert mock_reader_class.call_count == 2
            assert mock_reader.stream_data.call_count == 2
            
    finally:
        Path(config_file).unlink()