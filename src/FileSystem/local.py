"""
Local filesystem implementation.

This module provides a filesystem implementation for the local file system.
"""

import os
import shutil
from typing import List, IO
from pathlib import Path
import logging
import fsspec

from FileSystem.base import FileSystem


class LocalFileSystem(FileSystem):
    """
    Implementation of FileSystem for the local file system.
    
    This class provides methods for interacting with the local file system.
    It uses fsspec for some operations to ensure compatibility with the fsspec API.
    """
    
    def __init__(self) -> None:
        """Initialize the local file system."""
        self.logger = logging.getLogger(__name__)
        self.fs = fsspec.filesystem("file")
    
    def list_files(self, path_pattern: Path) -> List[Path]:
        """
        List files matching a pattern.
        
        Args:
            uri_pattern: A URI pattern to match files (e.g., "file:///data/input/**/*.csv")
            
        Returns:
            A list of URIs for files that match the pattern
        """
        self.logger.debug(f"Listing files with pattern: {path_pattern}")
        
        # fsspec's glob works with string paths
        paths = self.fs.glob(str(path_pattern))
        
        # Filter to include only files and convert back to Path objects
        file_paths = [Path(p) for p in paths if self.fs.isfile(p)]
        
        self.logger.debug(f"Found {len(file_paths)} files")
        return file_paths
    
    def open_input_stream(self, path: Path) -> IO:
        """
        Open a file for reading.
        
        Args:
            uri: The URI of the file to open
            
        Returns:
            An IO object for reading the file
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If the file cannot be opened
        """
        self.logger.debug(f"Opening input stream for: {path}")
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        return open(path, "rb")
    
    def open_output_stream(self, path: Path, mode: str = 'wb') -> IO:
        """
        Open a file for writing.
        
        Args:
            uri: The URI of the file to open
            mode: The mode to open the file in (default: 'wb')
            
        Returns:
            An IO object for writing to the file
            
        Raises:
            IOError: If the file cannot be opened for writing
        """
        self.logger.debug(f"Opening output stream for: {path}")
        
        # Ensure the parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
        
        return open(path, mode)
    
    def exists(self, path: Path) -> bool:
        """
        Check if a file exists.
        
        Args:
            uri: The URI of the file to check
            
        Returns:
            True if the file exists, False otherwise
        """
        return path.exists()
    
    def mkdirs(self, path: Path) -> None:
        """
        Create directories.
        
        Creates the specified directory and any parent directories that don't exist.
        
        Args:
            uri: The URI of the directory to create
            
        Raises:
            IOError: If the directories cannot be created
        """
        path.mkdir(parents=True, exist_ok=True)
    
    def remove(self, path: Path, recursive: bool = False) -> None:
        """
        Remove a file or directory.
        
        Args:
            uri: The URI of the file or directory to remove
            recursive: If True and the URI is a directory, remove it recursively
            
        Raises:
            FileNotFoundError: If the file or directory does not exist
            IOError: If the file or directory cannot be removed
        """
        if not path.exists():
            raise FileNotFoundError(f"Path not found: {path}")
        
        if path.is_dir():
            if recursive:
                shutil.rmtree(path)
            else:
                os.rmdir(path)
        else:
            path.unlink()
    
