import logging

logger = logging.getLogger(__name__)
from typing import List, Optional, Tuple
from datetime import datetime

def extract_partition_info(dates: List[Optional[datetime]]) -> List[Tuple[int, int, int]]:
    """
    Extract partition information (year, month, day) from a list of dates.
    
    Args:
        dates: A list of datetime objects
        
    Returns:
        A list of (year, month, day) tuples
        
    Raises:
        ValueError: If a date is None
    """
    partition_info = []
    
    for i, dt in enumerate(dates):
        if dt is None:
            logger.warning(f"Missing date in row {i}")
            # Use a default date (1970-01-01) for missing dates
            partition_info.append((1970, 1, 1))
        else:
            partition_info.append((dt.year, dt.month, dt.day))
    
    return partition_info


def get_partition_path(
    base_path: str, tenant: str, dataset: str, year: int, month: int, day: int
) -> str:
    """
    Get the path for a partition.
    
    Args:
        base_path: The base path for the output
        dataset: The name of the dataset (e.g., "costs_raw" or "tags_index")
        year: The year
        month: The month
        day: The day
        
    Returns:
        The partition path
    """
    return f"{base_path}/{tenant}/parquet/{dataset}/year={year}/month={month:02d}/day={day:02d}"
