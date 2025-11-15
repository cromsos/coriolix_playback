"""
Command Line Interface for the CORIOLIX Playback System.

Provides a CLI tool to stream timeseries data files using various protocols
(UDP broadcast, UDP unicast, TCP) with configurable options.
"""
import argparse
import sys
from pathlib import Path

from .timeseries_reader import TimeseriesReader
from .config import execute_config


def parse_arguments(args=None):
    """
    Parse command line arguments for the timeseries streamer.
    
    Args:
        args (list, optional): List of arguments. Uses sys.argv if None.
        
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='CORIOLIX Playback - Stream timeseries data files using various protocols',
        prog='coriolix-playback'
    )
    
    # Create subparsers for different command modes
    subparsers = parser.add_subparsers(
        dest='command',
        help='Command modes',
        required=True
    )
    
    # Single stream command
    stream_parser = subparsers.add_parser(
        'stream',
        help='Stream a single file'
    )
    
    # Required arguments for stream
    stream_parser.add_argument(
        '--file', '-f',
        required=True,
        help='Path to the timeseries data file (CSV, JSON, or CRLX)'
    )
    
    stream_parser.add_argument(
        '--protocol', '-p',
        choices=['tcp', 'udp_broadcast', 'udp_unicast'],
        required=True,
        help='Streaming protocol to use'
    )
    
    stream_parser.add_argument(
        '--port',
        type=int,
        required=True,
        help='Port number for streaming'
    )
    
    # Protocol-specific arguments
    stream_parser.add_argument(
        '--host',
        help='Host address for TCP protocol'
    )
    
    stream_parser.add_argument(
        '--broadcast-addr',
        help='Broadcast address for UDP broadcast protocol'
    )
    
    stream_parser.add_argument(
        '--unicast-addr',
        help='Target address for UDP unicast protocol'
    )
    
    # Optional streaming configuration
    stream_parser.add_argument(
        '--sensor-id',
        help='Override sensor ID in messages'
    )
    
    stream_parser.add_argument(
        '--no-update-timestamp',
        action='store_true',
        help='Keep original timestamps (default: update to current time)'
    )
    
    stream_parser.add_argument(
        '--raw-data-only',
        action='store_true',
        help='Send only raw data (3rd component), stripping timestamp and sensor_id'
    )
    
    stream_parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval between messages in seconds (default: 1.0)'
    )
    
    # Config file command
    config_parser = subparsers.add_parser(
        'config',
        help='Stream multiple files using a YAML configuration file'
    )
    
    config_parser.add_argument(
        '--file', '-f',
        required=True,
        help='Path to the YAML configuration file'
    )
    
    parsed_args = parser.parse_args(args)
    
    # Post-processing for stream command
    if parsed_args.command == 'stream':
        parsed_args.update_timestamp = not parsed_args.no_update_timestamp
        
        # Validation
        if parsed_args.protocol == 'tcp' and not parsed_args.host:
            parser.error('--host is required for TCP protocol')
        
        if parsed_args.protocol == 'udp_broadcast' and not parsed_args.broadcast_addr:
            parser.error('--broadcast-addr is required for UDP broadcast protocol')
            
        if parsed_args.protocol == 'udp_unicast' and not parsed_args.unicast_addr:
            parser.error('--unicast-addr is required for UDP unicast protocol')
        
        # Validate file exists
        if not Path(parsed_args.file).exists():
            parser.error(f'File not found: {parsed_args.file}')
    
    elif parsed_args.command == 'config':
        # Validate config file exists
        if not Path(parsed_args.file).exists():
            parser.error(f'Config file not found: {parsed_args.file}')
    
    return parsed_args


def execute_streaming(args):
    """
    Execute streaming based on parsed arguments.
    
    Args:
        args (argparse.Namespace): Parsed CLI arguments
        
    Returns:
        int: Number of records streamed
    """
    reader = TimeseriesReader(args.file)
    
    kwargs = {
        'protocol': args.protocol,
        'port': args.port,
        'interval': args.interval
    }
    
    # Add protocol-specific arguments
    if args.protocol == 'tcp':
        kwargs['host'] = args.host
    elif args.protocol == 'udp_broadcast':
        kwargs['broadcast_addr'] = args.broadcast_addr
        kwargs['sensor_id'] = args.sensor_id
        kwargs['update_timestamp'] = args.update_timestamp
        kwargs['raw_data_only'] = args.raw_data_only
    elif args.protocol == 'udp_unicast':
        kwargs['unicast_addr'] = args.unicast_addr
        kwargs['sensor_id'] = args.sensor_id
        kwargs['update_timestamp'] = args.update_timestamp
        kwargs['raw_data_only'] = args.raw_data_only
    
    count = reader.stream_data(**kwargs)
    return count


def main():
    """Main entry point for the CLI."""
    try:
        args = parse_arguments()
        
        if args.command == 'stream':
            print(f"Streaming {args.file} via {args.protocol} on port {args.port}")
            if args.protocol == 'udp_broadcast':
                print(f"Broadcasting to: {args.broadcast_addr}")
            elif args.protocol == 'udp_unicast':
                print(f"Unicasting to: {args.unicast_addr}")
            elif args.protocol == 'tcp':
                print(f"Connecting to: {args.host}")
                
            if args.sensor_id:
                print(f"Using sensor ID: {args.sensor_id}")
            if not args.update_timestamp:
                print("Preserving original timestamps")
                
            print(f"Interval: {args.interval} seconds")
            print("-" * 50)
            
            count = execute_streaming(args)
            print(f"✅ Successfully streamed {count} records")
            
        elif args.command == 'config':
            print(f"Loading configuration from: {args.file}")
            print("-" * 50)
            
            results = execute_config(args.file)
            
            total_records = sum(results.values())
            print(f"\n✅ Configuration executed successfully")
            print(f"Total records streamed: {total_records}")
            print("Results by stream:")
            for stream_name, count in results.items():
                print(f"  {stream_name}: {count} records")
        
    except KeyboardInterrupt:
        print("\n🛑 Streaming interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()