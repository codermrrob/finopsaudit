"""
Writer module for FOCUS ingestion service.

This module provides components for DuckDB
"""

from .duckdb_manager import DuckDBManager
from .dataAggregator import DataAggregator

__all__ = ["DuckDBManager", "DataAggregator"]
