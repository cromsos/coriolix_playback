"""
Configuration file parser and stream management for timeseries data streaming.

Supports YAML configuration files with multiple stream definitions,
defaults, and validation.
"""
import yaml
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
import threading
import time

from .timeseries_reader import TimeseriesReader


@dataclass
class StreamConfig:
    """Configuration for a single stream."""
    name: str
    file: str
    protocol: str
    port: int
    
    # Protocol-specific addresses
    host: Optional[str] = None
    broadcast_addr: Optional[str] = None
    unicast_addr: Optional[str] = None
    
    # Stream options
    sensor_id: Optional[str] = None
    update_timestamp: bool = True
    raw_data_only: bool = False
    interval: float = 1.0

    def validate(self):
        """Validate stream configuration."""
        if self.protocol == 'tcp' and not self.host:
            raise ValueError(f"Stream '{self.name}': host is required for TCP protocol")
        
        if self.protocol == 'udp_broadcast' and not self.broadcast_addr:
            raise ValueError(f"Stream '{self.name}': broadcast_addr is required for UDP broadcast protocol")
            
        if self.protocol == 'udp_unicast' and not self.unicast_addr:
            raise ValueError(f"Stream '{self.name}': unicast_addr is required for UDP unicast protocol")


@dataclass 
class Config:
    """Complete configuration with streams and defaults."""
    streams: List[StreamConfig]
    defaults: StreamConfig = field(default_factory=lambda: StreamConfig(
        name="defaults", file="", protocol="udp_broadcast", port=0
    ))


class ConfigParser:
    """Parser for YAML configuration files."""
    
    def parse(self, config_file: str) -> Config:
        """
        Parse YAML configuration file.
        
        Args:
            config_file (str): Path to YAML configuration file
            
        Returns:
            Config: Parsed configuration with validated streams
            
        Raises:
            ValueError: If configuration is invalid
            FileNotFoundError: If config file doesn't exist
        """
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
            
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if not data:
            raise ValueError("Empty configuration file")
            
        # Parse defaults
        defaults_data = data.get('defaults', {})
        defaults = StreamConfig(
            name="defaults",
            file="",
            protocol=defaults_data.get('protocol', 'udp_broadcast'),
            port=0,
            host=defaults_data.get('host'),
            broadcast_addr=defaults_data.get('broadcast_addr'),
            unicast_addr=defaults_data.get('unicast_addr'),
            sensor_id=defaults_data.get('sensor_id'),
            update_timestamp=defaults_data.get('update_timestamp', True),
            interval=defaults_data.get('interval', 1.0)
        )
        
        # Parse streams
        streams_data = data.get('streams', {})
        if not streams_data:
            raise ValueError("No streams defined in configuration")
            
        streams = []
        
        # Handle both dictionary format (stream_name: {config}) and list format
        if isinstance(streams_data, dict):
            # Dictionary format: streams: {stream_name: {file: ..., protocol: ...}}
            for stream_name, stream_data in streams_data.items():
                # Apply defaults
                stream_config = StreamConfig(
                    name=stream_name,
                    file=stream_data['file'],
                    protocol=stream_data['protocol'],
                    port=stream_data['port'],
                    host=stream_data.get('host', defaults.host),
                    broadcast_addr=stream_data.get('broadcast_addr', defaults.broadcast_addr),
                    unicast_addr=stream_data.get('unicast_addr', defaults.unicast_addr),
                    sensor_id=stream_data.get('sensor_id', defaults.sensor_id),
                    update_timestamp=stream_data.get('update_timestamp', defaults.update_timestamp),
                    raw_data_only=stream_data.get('raw_data_only', defaults.raw_data_only),
                    interval=stream_data.get('interval', defaults.interval)
                )
                
                # Validate stream
                stream_config.validate()
                streams.append(stream_config)
                
        else:
            # List format: streams: [{name: ..., file: ..., protocol: ...}]
            for stream_data in streams_data:
                # Apply defaults
                stream_config = StreamConfig(
                    name=stream_data['name'],
                    file=stream_data['file'],
                    protocol=stream_data['protocol'],
                    port=stream_data['port'],
                    host=stream_data.get('host', defaults.host),
                    broadcast_addr=stream_data.get('broadcast_addr', defaults.broadcast_addr),
                    unicast_addr=stream_data.get('unicast_addr', defaults.unicast_addr),
                    sensor_id=stream_data.get('sensor_id', defaults.sensor_id),
                    update_timestamp=stream_data.get('update_timestamp', defaults.update_timestamp),
                    raw_data_only=stream_data.get('raw_data_only', defaults.raw_data_only),
                    interval=stream_data.get('interval', defaults.interval)
                )
                
                # Validate stream
                stream_config.validate()
                streams.append(stream_config)
            
        return Config(streams=streams, defaults=defaults)


def execute_config(config_file: str, parallel: bool = True) -> Dict[str, int]:
    """
    Execute streams from configuration file.
    
    Args:
        config_file (str): Path to configuration file
        parallel (bool): Whether to run streams in parallel
        
    Returns:
        Dict[str, int]: Mapping of stream name to number of records sent
    """
    parser = ConfigParser()
    config = parser.parse(config_file)
    
    if parallel:
        results_list = _execute_streams_parallel(config.streams)
    else:
        results_list = _execute_streams_sequential(config.streams)
    
    # Convert list of results to simple dict mapping stream name -> record count
    results_dict = {}
    for result in results_list:
        if result['success']:
            results_dict[result['name']] = result['records_sent']
        else:
            # For failed streams, show 0 records and potentially log error
            results_dict[result['name']] = 0
            if result['error']:
                print(f"⚠️  Stream '{result['name']}' failed: {result['error']}")
    
    return results_dict


def _execute_streams_sequential(streams: List[StreamConfig]) -> List[Dict[str, Any]]:
    """Execute streams one after another."""
    results = []
    
    for stream in streams:
        result = _execute_single_stream(stream)
        results.append(result)
        
    return results


def _execute_streams_parallel(streams: List[StreamConfig]) -> List[Dict[str, Any]]:
    """Execute streams in parallel using threads."""
    results = {}
    threads = []
    
    def worker(stream: StreamConfig):
        results[stream.name] = _execute_single_stream(stream)
    
    # Start all threads
    for stream in streams:
        thread = threading.Thread(target=worker, args=(stream,))
        thread.start()
        threads.append(thread)
    
    # Wait for all to complete
    for thread in threads:
        thread.join()
    
    # Return results in original order as list
    return [results[stream.name] for stream in streams]


def _execute_single_stream(stream: StreamConfig) -> Dict[str, Any]:
    """Execute a single stream configuration."""
    result = {
        'name': stream.name,
        'success': False,
        'records_sent': 0,
        'error': None
    }
    
    try:
        reader = TimeseriesReader(stream.file)
        
        # Build streaming arguments
        kwargs = {
            'protocol': stream.protocol,
            'port': stream.port,
            'interval': stream.interval
        }
        
        # Add protocol-specific arguments
        if stream.protocol == 'tcp':
            kwargs['host'] = stream.host
        elif stream.protocol == 'udp_broadcast':
            kwargs['broadcast_addr'] = stream.broadcast_addr
            kwargs['sensor_id'] = stream.sensor_id
            kwargs['update_timestamp'] = stream.update_timestamp
            kwargs['raw_data_only'] = stream.raw_data_only
        elif stream.protocol == 'udp_unicast':
            kwargs['unicast_addr'] = stream.unicast_addr
            kwargs['sensor_id'] = stream.sensor_id
            kwargs['update_timestamp'] = stream.update_timestamp
            kwargs['raw_data_only'] = stream.raw_data_only
        
        # Execute streaming
        count = reader.stream_data(**kwargs)
        
        result['success'] = True
        result['records_sent'] = count
        
    except Exception as e:
        result['error'] = str(e)
    
    return result