"""
Tests for advanced streaming functionality of TimeseriesReader.

Tests UDP broadcast, UDP unicast, and other streaming protocols
with CRLX format output and timestamp/sensor_id modifications.
"""
import socket
import threading
import time
import pytest
from datetime import datetime

from timeseries_data_reader.timeseries_reader import TimeseriesReader


def test_can_stream_via_udp_broadcast(sample_crlx_file):
    """
    Test UDP broadcast streaming with CRLX format output.
    
    The stream should:
    1. Send messages in original CRLX format
    2. Update timestamps to current time by default
    3. Keep original sensor_id by default
    4. Broadcast on specified address and port
    """
    # Setup UDP receiver for broadcast
    messages = []
    port = 12345  # Test port
    broadcast_addr = "127.0.0.1"  # Localhost for testing
    
    def udp_receiver():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", port))
        sock.settimeout(2.0)  # 2 second timeout
        
        try:
            while True:
                data, addr = sock.recvfrom(1024)
                messages.append(data.decode('utf-8').strip())
        except socket.timeout:
            pass
        finally:
            sock.close()
    
    # Start receiver in background
    receiver_thread = threading.Thread(target=udp_receiver, daemon=True)
    receiver_thread.start()
    
    # Give receiver time to start
    time.sleep(0.1)
    
    # Stream data via UDP broadcast
    reader = TimeseriesReader(str(sample_crlx_file))
    count = reader.stream_data(
        protocol='udp_broadcast',
        port=port,
        broadcast_addr=broadcast_addr,
        update_timestamp=True,
        interval=0.1
    )
    
    # Wait for receiver to collect messages
    receiver_thread.join(timeout=3.0)
    
    # Verify results
    assert count == 4  # Should have sent 4 records
    assert len(messages) == 4  # Should have received 4 messages
    
    # Check first message format (should be CRLX format)
    first_msg = messages[0]
    parts = first_msg.split(' ', 2)
    assert len(parts) == 3  # timestamp, sensor_id, raw_data
    
    # Timestamp should be updated (different from original)
    assert parts[0] != '2025-11-14T00:05:36.704224Z'  # Original timestamp
    
    # Sensor ID should be preserved by default
    assert parts[1] == 'transm002005'
    
    # Raw data should be preserved
    assert 'CST-2005DR' in parts[2]


def test_can_stream_via_udp_unicast(sample_crlx_file):
    """
    Test UDP unicast streaming with custom sensor_id modification.
    """
    # Setup UDP receiver for unicast
    messages = []
    port = 12346  # Different port
    target_addr = "127.0.0.1"
    
    def udp_receiver():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((target_addr, port))
        sock.settimeout(2.0)
        
        try:
            while True:
                data, addr = sock.recvfrom(1024)
                messages.append(data.decode('utf-8').strip())
        except socket.timeout:
            pass
        finally:
            sock.close()
    
    receiver_thread = threading.Thread(target=udp_receiver, daemon=True)
    receiver_thread.start()
    time.sleep(0.1)
    
    # Stream with custom sensor_id
    reader = TimeseriesReader(str(sample_crlx_file))
    count = reader.stream_data(
        protocol='udp_unicast',
        port=port,
        unicast_addr=target_addr,
        sensor_id='custom_sensor_999',
        update_timestamp=False,  # Keep original timestamps
        interval=0.1
    )
    
    receiver_thread.join(timeout=3.0)
    
    # Verify results
    assert count == 4
    assert len(messages) == 4
    
    # Check sensor_id was modified
    first_msg = messages[0]
    parts = first_msg.split(' ', 2)
    assert parts[1] == 'custom_sensor_999'
    
    # Timestamp should be original (update_timestamp=False)
    assert parts[0] == '2025-11-14T00:05:36.704224Z'