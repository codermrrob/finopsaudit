"""
Base filesystem abstraction.

This module defines the abstract base class for filesystem implementations.
"""

from abc import ABC, abstractmethod
from typing import List, IO
from pathlib import Path


class FileSystem(ABC):
    """
    Abstract base class for filesystem implementations.
    
    This class defines the interface that all filesystem implementations must follow.
    It provides methods for listing, reading, and writing files.
    """
    
    @abstractmethod
    def list_files(self, path_pattern: Path) -> List[Path]:
        """
        List files matching a pattern.
        
        Args:
            path_pattern: A Path object with a glob pattern
            
        Returns:
            A list of Path objects for files that match the pattern
        """
        pass
    
    @abstractmethod
    def open_input_stream(self, path: Path) -> IO:
        """
        Open a file for reading.
        
        Args:
            path: The Path of the file to open
            
        Returns:
            An IO object for reading the file
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If the file cannot be opened
        """
        pass
    
    @abstractmethod
    def open_output_stream(self, path: Path, mode: str = 'wb') -> IO:
        """
        Open a file for writing.
        
        Args:
            path: The Path of the file to open
            mode: The mode to open the file in (default: 'wb')
            
        Returns:
            An IO object for writing to the file
            
        Raises:
            IOError: If the file cannot be opened for writing
        """
        pass
    
    @abstractmethod
    def exists(self, uri: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            uri: The URI of the file to check
            
        Returns:
            True if the file exists, False otherwise
        """
        pass
    
    @abstractmethod
    def mkdirs(self, uri: str) -> None:
        """
        Create directories.
        
        Creates the specified directory and any parent directories that don't exist.
        
        Args:
            uri: The URI of the directory to create
            
        Raises:
            IOError: If the directories cannot be created
        """
        pass
    
    @abstractmethod
    def remove(self, uri: str, recursive: bool = False) -> None:
        """
        Remove a file or directory.
        
        Args:
            uri: The URI of the file or directory to remove
            recursive: If True and the URI is a directory, remove it recursively
            
        Raises:
            FileNotFoundError: If the file or directory does not exist
            IOError: If the file or directory cannot be removed
        """
        pass
