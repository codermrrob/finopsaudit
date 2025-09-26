"""
Logging utilities for FOCUS ingestion service.

This module provides utilities for setting up logging with rotation.
"""

import logging
import os
import sys
import codecs

def setup_logging(
    log_dir: str, 
    log_level: int = logging.INFO,
    log_file_name: str = "focus_ingest.log",
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 10
) -> None:
    """
    Set up logging with rotation.
    
    Args:
        log_dir: The directory to store log files in
        log_level: The logging level (default: logging.INFO)
        log_file_name: The name of the log file (default: "focus_ingest.log")
        max_bytes: The maximum size of each log file in bytes (default: 1 MB)
        backup_count: The number of backup files to keep (default: 5)
    """
    # Create the log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, log_file_name)
    
    # Create formatters for file and console
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Set up file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    # Original RotatingFileHandler with encoding:
    # from logging.handlers import RotatingFileHandler
    # file_handler = RotatingFileHandler(
    #     log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8'
    # )
    file_handler.setFormatter(file_formatter)
    
    # Set up console handler with UTF-8 encoding
    # Wrap sys.stdout to ensure UTF-8 encoding, especially on Windows
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add the new handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Log the configuration
    logging.info(f"Logging configured with level {logging.getLevelName(log_level)}")
    logging.info(f"Log file: {log_file}")
    logging.info(f"Max log file size: {max_bytes} bytes")
    logging.info(f"Backup count: {backup_count}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: The name of the logger
        
    Returns:
        A logger instance
    """
    return logging.getLogger(name)
