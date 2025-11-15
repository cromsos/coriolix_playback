"""
TimeseriesReader module for reading pre-recorded timeseries data.

This module provides functionality to read timeseries data from various formats
and serve it as dummy data input to data acquisition systems.
"""
import csv
import json
from pathlib import Path
import socket
import time


class TimeseriesReader:
    """
    A reader for timeseries data from various file formats.
    
    This class provides methods to read timeseries data from CSV, JSON,
    and other formats for use as dummy data input.
    """
    
    def __init__(self, file_path=None):
        """
        Initialize the TimeseriesReader.
        
        Args:
            file_path (str, optional): Path to the timeseries data file.
        """
        self.file_path = file_path
    
    def read_data(self, file_path=None):
        """
        Read data from the specified file.
        
        Args:
            file_path (str, optional): Path to file. Uses instance file_path if not provided.
            
        Returns:
            list: Parsed data records from the file
            
        Raises:
            ValueError: If no file path is provided
            FileNotFoundError: If the file doesn't exist
        """
        if file_path is None:
            file_path = self.file_path
        if file_path is None:
            raise ValueError("No file path provided")
        
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Determine file type and parse accordingly
        suffix = file_path_obj.suffix.lower()
        if suffix == '.csv':
            return self._read_csv(file_path)
        elif suffix == '.json':
            return self._read_json(file_path)
        elif suffix == '.crlx':
            return self._read_crlx(file_path)
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
    
    def _read_csv(self, file_path):
        """
        Read CSV file and return parsed records.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            list: List of dictionaries representing CSV records
            
        Raises:
            csv.Error: If CSV parsing fails
        """
        try:
            records = []
            with open(file_path, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    records.append(row)
            return records
        except csv.Error as e:
            raise ValueError(f"Error reading CSV file {file_path}: {e}")
    
    def _read_json(self, file_path):
        """
        Read JSON file and return parsed records.
        
        Args:
            file_path (str): Path to the JSON file
            
        Returns:
            list: List of dictionaries representing JSON records
            
        Raises:
            json.JSONDecodeError: If JSON parsing fails
            ValueError: If JSON format is unexpected
        """
        try:
            with open(file_path, 'r') as jsonfile:
                data = json.load(jsonfile)
                # Ensure we return a list even if JSON contains a single object
                if isinstance(data, dict):
                    return [data]
                elif isinstance(data, list):
                    return data
                else:
                    raise ValueError(f"Unexpected JSON format in {file_path}: expected dict or list, got {type(data)}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error reading JSON file {file_path}: {e}")

    def _read_crlx(self, file_path):
        """
        Read CRLX file and return parsed records.
        
        Format: <ISO8601Datetime><space><SensorID><space><RAW DATA MESSAGE>
        
        Args:
            file_path (str): Path to the CRLX file

        Returns:
            list: List of dictionaries representing CRLX records

        Raises:
            ValueError: If CRLX parsing fails
        """
        try:
            records = []
            with open(file_path, 'r') as crlxfile:
                for line in crlxfile:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Split on space to get timestamp, sensor_id, and raw_data
                    parts = line.split(' ', 2)  # Split on first 2 spaces only
                    if len(parts) != 3:
                        raise ValueError(f"Invalid CRLX line format: {line}")
                    
                    timestamp, sensor_id, raw_data = parts
                    
                    record = {
                        'timestamp': timestamp,
                        'sensor_id': sensor_id,
                        'raw_data': raw_data
                    }
                    records.append(record)
            return records
        except Exception as e:
            raise ValueError(f"Error reading CRLX file {file_path}: {e}")

    def stream_data(self, host: str = None, port: int = None, interval: float = 0.0, 
                   protocol: str = 'tcp', broadcast_addr: str = None, 
                   unicast_addr: str = None, sensor_id: str = None, 
                   update_timestamp: bool = True):
        """
        Stream parsed records using various protocols.

        Args:
            host (str, optional): Legacy TCP host parameter
            port (int): Port number
            interval (float): Seconds to wait between sending records
            protocol (str): Protocol to use ('tcp', 'udp_broadcast', 'udp_unicast')
            broadcast_addr (str, optional): Broadcast address for UDP broadcast
            unicast_addr (str, optional): Target address for UDP unicast
            sensor_id (str, optional): Override sensor_id in messages
            update_timestamp (bool): Whether to update timestamps to current time

        Returns:
            int: Number of records sent

        Raises:
            ConnectionError: If unable to connect/send
            ValueError: If invalid protocol or missing required parameters
        """
        if protocol == 'tcp':
            # Legacy TCP streaming (backward compatibility)
            return self._stream_tcp(host, port, interval)
        elif protocol == 'udp_broadcast':
            return self._stream_udp_broadcast(broadcast_addr, port, interval, 
                                            sensor_id, update_timestamp)
        elif protocol == 'udp_unicast':
            return self._stream_udp_unicast(unicast_addr, port, interval,
                                          sensor_id, update_timestamp)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")
    
    def _stream_tcp(self, host: str, port: int, interval: float):
        """Legacy TCP streaming implementation."""
        records = self.read_data()

        try:
            with socket.create_connection((host, port)) as sock:
                for rec in records:
                    line = json.dumps(rec, ensure_ascii=False) + "\n"
                    sock.sendall(line.encode("utf-8"))
                    if interval > 0:
                        time.sleep(interval)
        except OSError as e:
            raise ConnectionError(f"Failed to connect/send to {host}:{port}: {e}")

        return len(records)
    
    def _stream_udp_broadcast(self, broadcast_addr: str, port: int, interval: float,
                            sensor_id_override: str = None, update_timestamp: bool = True):
        """Stream data via UDP broadcast in CRLX format."""
        if not broadcast_addr:
            raise ValueError("broadcast_addr is required for UDP broadcast")
        if not port:
            raise ValueError("port is required")
            
        records = self.read_data()
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            
            for rec in records:
                # Convert record back to CRLX format
                crlx_line = self._format_as_crlx(rec, sensor_id_override, update_timestamp)
                sock.sendto(crlx_line.encode('utf-8'), (broadcast_addr, port))
                
                if interval > 0:
                    time.sleep(interval)
                    
            sock.close()
            
        except OSError as e:
            raise ConnectionError(f"Failed to broadcast to {broadcast_addr}:{port}: {e}")
            
        return len(records)
    
    def _stream_udp_unicast(self, unicast_addr: str, port: int, interval: float,
                          sensor_id_override: str = None, update_timestamp: bool = True):
        """Stream data via UDP unicast in CRLX format."""
        if not unicast_addr:
            raise ValueError("unicast_addr is required for UDP unicast")
        if not port:
            raise ValueError("port is required")
            
        records = self.read_data()
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            for rec in records:
                # Convert record back to CRLX format
                crlx_line = self._format_as_crlx(rec, sensor_id_override, update_timestamp)
                sock.sendto(crlx_line.encode('utf-8'), (unicast_addr, port))
                
                if interval > 0:
                    time.sleep(interval)
                    
            sock.close()
            
        except OSError as e:
            raise ConnectionError(f"Failed to send to {unicast_addr}:{port}: {e}")
            
        return len(records)
    
    def _format_as_crlx(self, record: dict, sensor_id_override: str = None, 
                       update_timestamp: bool = True):
        """Format a record back to CRLX format string."""
        from datetime import datetime
        
        # Handle timestamp
        if update_timestamp:
            timestamp = datetime.utcnow().isoformat() + 'Z'
        else:
            timestamp = record['timestamp']
            
        # Handle sensor_id
        if sensor_id_override:
            sensor_id = sensor_id_override
        else:
            sensor_id = record['sensor_id']
            
        # Reconstruct CRLX line
        raw_data = record['raw_data']
        return f"{timestamp} {sensor_id} {raw_data}"