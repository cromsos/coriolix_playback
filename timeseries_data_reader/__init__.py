"""
Timeseries Data Reader - A utility for reading pre-recorded timeseries data.

This package provides functionality to read timeseries data from various formats
(CSV, JSON, CRLX) and serve it as "dummy" data input to data acquisition systems.
"""

__version__ = "0.1.0"
__author__ = "Your Name"

from .timeseries_reader import TimeseriesReader

__all__ = ['TimeseriesReader']