"""
Tests for the CLI interface of the timeseries streamer.

Tests argument parsing, validation, and execution of streaming commands.
"""
import pytest
import argparse
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from timeseries_data_reader.cli import main, parse_arguments, execute_streaming


def test_parse_arguments_basic():
    """Test basic argument parsing for UDP broadcast."""
    # Create a temporary file for the test
    with tempfile.NamedTemporaryFile(mode='w', suffix='.crlx', delete=False) as f:
        f.write("test data")
        temp_file = f.name
    
    try:
        args = parse_arguments([
            'stream',
            '--file', temp_file,
            '--protocol', 'udp_broadcast', 
            '--port', '8080',
            '--broadcast-addr', '192.168.1.255'
        ])
        
        assert args.command == 'stream'
        assert args.file == temp_file
        assert args.protocol == 'udp_broadcast'
        assert args.port == 8080
        assert args.broadcast_addr == '192.168.1.255'
        assert args.interval == 1.0  # default
        assert args.update_timestamp is True  # default
    finally:
        Path(temp_file).unlink()  # Clean up


def test_parse_arguments_udp_unicast():
    """Test argument parsing for UDP unicast with custom options."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.crlx', delete=False) as f:
        f.write("test data")
        temp_file = f.name
    
    try:
        args = parse_arguments([
            'stream',
            '--file', temp_file,
            '--protocol', 'udp_unicast',
            '--port', '8081', 
            '--unicast-addr', '192.168.1.100',
            '--sensor-id', 'custom_sensor_001',
            '--no-update-timestamp',
            '--interval', '0.5'
        ])
        
        assert args.command == 'stream'
        assert args.file == temp_file
        assert args.protocol == 'udp_unicast'
        assert args.port == 8081
        assert args.unicast_addr == '192.168.1.100'
        assert args.sensor_id == 'custom_sensor_001'
        assert args.update_timestamp is False
        assert args.interval == 0.5
    finally:
        Path(temp_file).unlink()


def test_parse_arguments_tcp_legacy():
    """Test argument parsing for legacy TCP mode."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.crlx', delete=False) as f:
        f.write("test data")
        temp_file = f.name
    
    try:
        args = parse_arguments([
            'stream',
            '--file', temp_file,
            '--protocol', 'tcp',
            '--host', 'localhost',
            '--port', '9090'
        ])
        
        assert args.command == 'stream'
        assert args.protocol == 'tcp'
        assert args.host == 'localhost'
        assert args.port == 9090
    finally:
        Path(temp_file).unlink()


def test_parse_arguments_validation_errors():
    """Test that argument parsing fails with invalid combinations."""
    
    # Missing required broadcast address
    with pytest.raises(SystemExit):
        parse_arguments([
            'stream',
            '--file', 'test.crlx',
            '--protocol', 'udp_broadcast',
            '--port', '8080'
            # missing --broadcast-addr
        ])
    
    # Missing required unicast address  
    with pytest.raises(SystemExit):
        parse_arguments([
            'stream',
            '--file', 'test.crlx', 
            '--protocol', 'udp_unicast',
            '--port', '8080'
            # missing --unicast-addr
        ])


def test_execute_streaming_udp_broadcast(sample_crlx_file):
    """Test CLI execution of UDP broadcast streaming."""
    
    # Mock the TimeseriesReader to avoid actual network traffic
    with patch('timeseries_data_reader.cli.TimeseriesReader') as mock_reader_class:
        mock_reader = MagicMock()
        mock_reader.stream_data.return_value = 4
        mock_reader_class.return_value = mock_reader
        
        # Create arguments object
        args = argparse.Namespace(
            command='stream',
            file=str(sample_crlx_file),
            protocol='udp_broadcast',
            port=8080,
            broadcast_addr='192.168.1.255',
            unicast_addr=None,
            host=None,
            sensor_id=None,
            update_timestamp=True,
            interval=1.0
        )
        
        # Execute streaming
        result = execute_streaming(args)
        
        # Verify TimeseriesReader was used correctly
        mock_reader_class.assert_called_once_with(str(sample_crlx_file))
        mock_reader.stream_data.assert_called_once_with(
            protocol='udp_broadcast',
            port=8080,
            broadcast_addr='192.168.1.255',
            sensor_id=None,
            update_timestamp=True,
            interval=1.0
        )
        
        assert result == 4  # Number of records streamed


def test_execute_streaming_udp_unicast(sample_crlx_file):
    """Test CLI execution of UDP unicast streaming."""
    
    with patch('timeseries_data_reader.cli.TimeseriesReader') as mock_reader_class:
        mock_reader = MagicMock()
        mock_reader.stream_data.return_value = 4
        mock_reader_class.return_value = mock_reader
        
        args = argparse.Namespace(
            command='stream',
            file=str(sample_crlx_file),
            protocol='udp_unicast', 
            port=8081,
            broadcast_addr=None,
            unicast_addr='192.168.1.100',
            host=None,
            sensor_id='test_sensor_999',
            update_timestamp=False,
            interval=0.5
        )
        
        result = execute_streaming(args)
        
        mock_reader.stream_data.assert_called_once_with(
            protocol='udp_unicast',
            port=8081,
            unicast_addr='192.168.1.100', 
            sensor_id='test_sensor_999',
            update_timestamp=False,
            interval=0.5
        )
        
        assert result == 4


def test_main_integration(sample_crlx_file):
    """Test main CLI function integration."""
    
    with patch('timeseries_data_reader.cli.TimeseriesReader') as mock_reader_class:
        mock_reader = MagicMock()
        mock_reader.stream_data.return_value = 4
        mock_reader_class.return_value = mock_reader
        
        # Test via main function with sys.argv simulation
        test_args = [
            'timeseries-streamer',  # program name
            'stream',
            '--file', str(sample_crlx_file),
            '--protocol', 'udp_broadcast',
            '--port', '8080', 
            '--broadcast-addr', '127.0.0.1'
        ]
        
        with patch('sys.argv', test_args):
            main()
            
        # Verify it was called correctly
        mock_reader_class.assert_called_once_with(str(sample_crlx_file))
        mock_reader.stream_data.assert_called_once()


def test_parse_arguments_config():
    """Test argument parsing for config command."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("streams: {}")
        temp_file = f.name
    
    try:
        args = parse_arguments([
            'config',
            '--file', temp_file
        ])
        
        assert args.command == 'config'
        assert args.file == temp_file
    finally:
        Path(temp_file).unlink()


def test_main_config_integration():
    """Test main CLI function with config command."""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("streams: {}")
        temp_config = f.name
    
    try:
        with patch('timeseries_data_reader.cli.execute_config') as mock_execute_config:
            mock_execute_config.return_value = {'stream1': 4, 'stream2': 6}
            
            # Test via main function with sys.argv simulation
            test_args = [
                'timeseries-streamer',  # program name
                'config',
                '--file', temp_config
            ]
            
            with patch('sys.argv', test_args):
                main()
                
            # Verify config execution was called
            mock_execute_config.assert_called_once_with(temp_config)
    finally:
        Path(temp_config).unlink()