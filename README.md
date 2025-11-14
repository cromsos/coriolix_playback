# CORIOLIX Playback

A Python utility for reading and streaming timeseries data files using various network protocols. Built for CORIOLIX data acquisition system development.

## Features

- **File Format Support**: CSV, JSON, and CRLX timeseries data
- **Multiple Protocols**: TCP, UDP Broadcast, UDP Unicast
- **Configuration Files**: YAML-based multi-stream configuration
- **Command Line Interface**: Easy-to-use CLI with subcommands
- **Test-Driven Development**: Comprehensive test coverage

## Installation

Install in development mode:

```bash
pip install -e .
```

## Data Setup

1. **Place your CRLX files** in the `data/` directory
2. **Create a configuration file** based on `config_template.yaml`
3. **Update network settings** to match your environment

```bash
# Example data directory structure
data/
├── sensor1.crlx
├── sensor2.crlx
├── my_config.yaml
└── .gitkeep
```

**Note**: CRLX data files are excluded from version control via `.gitignore` to avoid committing large data files.

## Usage

### Single File Streaming

Stream a single data file:

```bash
# UDP Broadcast
coriolix-playback stream --file data/sensor1.crlx --protocol udp_broadcast --port 8080 --broadcast-addr 192.168.1.255

# UDP Unicast
coriolix-playback stream --file data/sensor2.crlx --protocol udp_unicast --port 8081 --unicast-addr 192.168.1.100

# TCP
coriolix-playback stream --file data/legacy.csv --protocol tcp --port 9090 --host localhost
```

### Configuration File Streaming

For production deployment with multiple streams, use a YAML configuration file:

```bash
coriolix-playback config --file config.yaml
```

## Configuration File Format

Create a YAML file with multiple stream definitions:

```yaml
streams:
  sensor_data_1:
    file: "data/sensor1.crlx"
    protocol: "udp_broadcast"
    port: 8080
    broadcast_addr: "192.168.1.255"
    sensor_id: "SENSOR_001"
    interval: 1.0
    update_timestamp: true
    
  sensor_data_2:
    file: "data/sensor2.crlx"
    protocol: "udp_unicast"
    port: 8081
    unicast_addr: "192.168.1.100"
    sensor_id: "SENSOR_002"
    interval: 0.5
    update_timestamp: true
    
  legacy_tcp:
    file: "data/legacy.csv"
    protocol: "tcp"
    port: 9090
    host: "localhost"
    interval: 2.0
```

## Configuration Parameters

### Required Parameters

- **file**: Path to the timeseries data file
- **protocol**: One of `tcp`, `udp_broadcast`, `udp_unicast`
- **port**: Port number for streaming

### Protocol-Specific Parameters

#### TCP Protocol
- **host**: Target host address (required)

#### UDP Broadcast Protocol
- **broadcast_addr**: Broadcast address (required)
- **sensor_id**: Override sensor ID in messages (optional)
- **update_timestamp**: Update timestamps to current time (optional, default: true)

#### UDP Unicast Protocol
- **unicast_addr**: Target unicast address (required)
- **sensor_id**: Override sensor ID in messages (optional)
- **update_timestamp**: Update timestamps to current time (optional, default: true)

### General Parameters
- **interval**: Time interval between messages in seconds (optional, default: 1.0)

## File Formats

### CRLX Format
Custom binary format with timestamp, sensor ID, and 3-axis accelerometer data:
```
timestamp:1678901234.567,sensor_id:sensor_001,x:1.23,y:4.56,z:7.89
```

### CSV Format
Comma-separated values with headers:
```
timestamp,sensor_id,x,y,z
1678901234.567,sensor_001,1.23,4.56,7.89
```

### JSON Format
JSON Lines format (one JSON object per line):
```json
{"timestamp": 1678901234.567, "sensor_id": "sensor_001", "x": 1.23, "y": 4.56, "z": 7.89}
```

## Development

This project was built using Test-Driven Development (TDD). Run tests with:

```bash
python -m pytest
```

Current test coverage: 24 tests covering file reading, streaming protocols, CLI interface, and configuration parsing.