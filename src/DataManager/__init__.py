"""
Data management module for high-level file operations.

This module provides the DataManager class which offers high-level data
operations (read/write Parquet, CSV, JSON, YAML) built on top of the
FileSystem abstraction.
"""

from .DataManager import DataManager

__all__ = ["DataManager"]
