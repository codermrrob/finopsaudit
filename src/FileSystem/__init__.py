"""
Filesystem abstraction module for FOCUS ingestion service.

This module provides a filesystem abstraction layer that allows the service
to read from and write to different storage backends.
"""

from .base import FileSystem
from .local import LocalFileSystem
from .registry import get_filesystem, register_filesystem

__all__ = ["FileSystem", "LocalFileSystem", "get_filesystem", "register_filesystem"]