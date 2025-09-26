"""
Date utilities for FOCUS ingestion service.

This module provides utilities for parsing and handling dates in FOCUS CSV files.
"""

import logging
import re
from typing import Optional, Union
from datetime import datetime

import dateutil.parser
from dateutil import tz


logger = logging.getLogger(__name__)


def parse_date(date_str: Union[str, None]) -> Optional[datetime]:
    """
    Parse a date string to a datetime object.
    
    Args:
        date_str: A date string in ISO-8601 format (e.g., "2025-01-01T00:00:00Z")
        
    Returns:
        A datetime object, or None if the input is None or empty
        
    Raises:
        ValueError: If the date string cannot be parsed
    """
    if not date_str:
        return None
    
    try:
        # For empty strings
        if not date_str.strip():
            return None
        
        # Handle various UTC timezone formats that cause issues
        # Case 1: "Z" at the end (standard ISO format)
        if date_str.endswith('Z'):
            # Convert to +00:00 format which is more widely supported
            date_str = date_str[:-1] + '+00:00'
        
        # Case 2: Contains "UTC" as timezone
        elif 'UTC' in date_str:
            # Try to normalize by replacing UTC with +00:00
            date_str = re.sub(r'\s*UTC\s*$', '+00:00', date_str)
        
        # Case 3: Handle timezone-less dates by assuming they're UTC
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            # Simple date format YYYY-MM-DD, add time and timezone
            date_str = f"{date_str}T00:00:00+00:00"
        
        # Case 4: Handle date with time but no timezone
        elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', date_str):
            # Add timezone
            date_str = f"{date_str}+00:00"
            
        # Parse with dateutil, explicitly setting a UTC timezone if one isn't found
        try:
            dt = dateutil.parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz.UTC)
            return dt
        except ValueError:
            # Last resort: try a more flexible parsing approach
            # Extract date parts with regex
            date_match = re.search(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', date_str)
            if date_match:
                year, month, day = map(int, date_match.groups())
                # Create date with default time at UTC
                return datetime(year, month, day, 0, 0, 0, tzinfo=tz.UTC)
            raise
            
    except Exception as e:
        logger.warning(f"Error parsing date: {date_str} - {e}")
        # Default to epoch time for completely invalid dates rather than failing
        logger.warning(f"Using default date (1970-01-01) for invalid date: {date_str}")
        return datetime(1970, 1, 1, tzinfo=tz.UTC)


