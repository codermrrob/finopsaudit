"""
Filesystem registry.

This module provides a registry for filesystem implementations.
"""

import logging
from typing import Dict, Type

from FileSystem.base import FileSystem
from FileSystem.local import LocalFileSystem

# Registry of filesystem implementations
_FILESYSTEM_REGISTRY: Dict[str, Type[FileSystem]] = {}
logger = logging.getLogger(__name__)


def register_filesystem(name: str, fs_class: Type[FileSystem]) -> None:
    """
    Register a filesystem implementation.
    
    Args:
        name: The name of the filesystem implementation
        fs_class: The filesystem implementation class
    """
    logger.debug(f"Registering filesystem: {name}")
    _FILESYSTEM_REGISTRY[name] = fs_class


def get_filesystem(name: str, **kwargs) -> FileSystem:
    """
    Get a filesystem implementation by name.
    
    Args:
        name: The name of the filesystem implementation
        **kwargs: Additional arguments to pass to the filesystem constructor
        
    Returns:
        An instance of the requested filesystem implementation
        
    Raises:
        ValueError: If the requested filesystem implementation is not registered
    """
    logger.debug(f"Getting filesystem: {name}")
    
    if name not in _FILESYSTEM_REGISTRY:
        raise ValueError(f"Filesystem not registered: {name}")
    
    fs_class = _FILESYSTEM_REGISTRY[name]
    return fs_class(**kwargs)


# Register built-in filesystem implementations
register_filesystem("local", LocalFileSystem)

# Note: S3 and Azure filesystem implementations would be registered here
# if they were installed or imported from optional dependencies
