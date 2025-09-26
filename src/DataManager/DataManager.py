"""
Data management abstraction module.

This module provides high-level data operations (read/write various formats)
built on top of the FileSystem abstraction layer.
"""

import csv
import json
import logging
from typing import Dict, Any, List, Union
from io import StringIO
from pathlib import Path

import pandas as pd
import yaml

from FileSystem.base import FileSystem


class DataManager:
    """
    High-level data operations using a FileSystem backend.
    
    This class provides methods for reading and writing various data formats
    (Parquet, CSV, JSON, YAML) using the abstract FileSystem interface.
    """
    
    def __init__(self, filesystem: FileSystem) -> None:
        """
        Initialize the DataManager with a filesystem backend.
        
        Args:
            filesystem: The FileSystem implementation to use for storage operations
        """
        self.fs = filesystem
        self.logger = logging.getLogger(__name__)
    
    def read_parquet(self, path: Union[str, Path]) -> pd.DataFrame:
        """
        Read a Parquet file into a DataFrame.
        
        Args:
            path: The path of the Parquet file to read
            
        Returns:
            A pandas DataFrame containing the data from the file
        """
        path = Path(path)
        if not self.fs.exists(path):
            self.logger.debug(f"Parquet file not found: {path}, returning empty DataFrame")
            return pd.DataFrame()
        
        self.logger.debug(f"Reading Parquet file: {path}")
        with self.fs.open_input_stream(path) as stream:
            return pd.read_parquet(stream)
    
    def write_parquet(self, df: pd.DataFrame, path: Union[str, Path]) -> None:
        """
        Write a DataFrame to a Parquet file.
        
        Args:
            df: The DataFrame to write
            path: The path where the Parquet file should be written
        """
        path = Path(path)
        self.logger.debug(f"Writing Parquet file: {path}")
        
        with self.fs.open_output_stream(path, mode='wb') as stream:
            df.to_parquet(stream, index=False)
    
    def write_csv(self, df: pd.DataFrame, path: Union[str, Path], **kwargs) -> None:
        """
        Write a DataFrame to a CSV file.
        
        Args:
            df: The DataFrame to write
            path: The path where the CSV file should be written
            **kwargs: Additional arguments to pass to pandas to_csv
        """
        path = Path(path)
        self.logger.debug(f"Writing CSV file: {path}")
        
        with self.fs.open_output_stream(path, mode='wb') as stream:
            text_buffer = StringIO()
            df.to_csv(text_buffer, index=False, **kwargs)
            stream.write(text_buffer.getvalue().encode('utf-8'))
    
    def write_json(self, data: Dict[str, Any], path: Union[str, Path]) -> None:
        """
        Write a dictionary to a JSON file.
        
        Args:
            data: The dictionary to write
            path: The path where the JSON file should be written
        """
        path = Path(path)
        self.logger.debug(f"Writing JSON file: {path}")
        
        with self.fs.open_output_stream(path, mode='wb') as stream:
            json_str = json.dumps(data, indent=4, ensure_ascii=False)
            stream.write(json_str.encode('utf-8'))
    
    def write_empty_csv(self, columns: List[str], path: Union[str, Path]) -> None:
        """
        Write an empty CSV file with only a header row.
        
        Args:
            columns: List of column names for the header
            path: The path where the CSV file should be written
        """
        path = Path(path)
        self.logger.debug(f"Writing empty CSV file: {path}")
        
        with self.fs.open_output_stream(path, mode='wb') as stream:
            text_buffer = StringIO()
            writer = csv.writer(text_buffer)
            writer.writerow(columns)
            stream.write(text_buffer.getvalue().encode('utf-8'))
    
    def read_yaml(self, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Read a YAML file into a dictionary.
        
        Args:
            path: The path of the YAML file to read
            
        Returns:
            A dictionary containing the data from the file
        """
        path = Path(path)
        if not self.fs.exists(path):
            self.logger.debug(f"YAML file not found: {path}, returning empty dict")
            return {}
        
        self.logger.debug(f"Reading YAML file: {path}")
        
        try:
            with self.fs.open_input_stream(path) as stream:
                content = stream.read().decode('utf-8')
                data = yaml.safe_load(content)
                return data if data else {}
        except yaml.YAMLError as e:
            self.logger.warning(f"Invalid YAML in file {path}: {e}, returning empty dict")
            return {}
    
    def write_yaml(self, data: Dict[str, Any], path: Union[str, Path]) -> None:
        """
        Write a dictionary to a YAML file.
        
        Args:
            data: The dictionary to write
            path: The path where the YAML file should be written
        """
        path = Path(path)
        self.logger.debug(f"Writing YAML file: {path}")
        
        with self.fs.open_output_stream(path, mode='wb') as stream:
            yaml_str = yaml.dump(data, default_flow_style=False, sort_keys=False)
            stream.write(yaml_str.encode('utf-8'))
    
    def read_csv(self, path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """
        Read a CSV file into a DataFrame.
        
        Args:
            path: The path of the CSV file to read
            **kwargs: Additional arguments to pass to pandas read_csv
            
        Returns:
            A pandas DataFrame containing the data from the file
        """
        path = Path(path)
        if not self.fs.exists(path):
            self.logger.debug(f"CSV file not found: {path}, returning empty DataFrame")
            return pd.DataFrame()
        
        self.logger.debug(f"Reading CSV file: {path}")
        with self.fs.open_input_stream(path) as stream:
            return pd.read_csv(stream, **kwargs)
    
    def read_json(self, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Read a JSON file into a dictionary.
        
        Args:
            path: The path of the JSON file to read
            
        Returns:
            A dictionary containing the data from the file
        """
        path = Path(path)
        if not self.fs.exists(path):
            self.logger.debug(f"JSON file not found: {path}, returning empty dict")
            return {}
        
        self.logger.debug(f"Reading JSON file: {path}")
        
        try:
            with self.fs.open_input_stream(path) as stream:
                content = stream.read().decode('utf-8')
                return json.loads(content)
        except json.JSONDecodeError as e:
            self.logger.warning(f"Invalid JSON in file {path}: {e}, returning empty dict")
            return {}
    
    def copy_file(self, source_path: Union[str, Path], dest_path: Union[str, Path]) -> None:
        """
        Copy a file from a source path to a destination path.

        Args:
            source_path: The source file path
            dest_path: The destination file path
        """
        source_path = Path(source_path)
        dest_path = Path(dest_path)
        self.logger.debug(f"Copying file from {source_path} to {dest_path}")

        with self.fs.open_input_stream(source_path) as source_stream:
            with self.fs.open_output_stream(dest_path, mode='wb') as dest_stream:
                dest_stream.write(source_stream.read())