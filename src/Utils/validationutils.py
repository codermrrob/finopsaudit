from typing import Optional
from datetime import datetime, timedelta

def validate_charge_period(
    start_date: Optional[datetime], end_date: Optional[datetime]) -> bool:
    """
    Validate that the charge period is valid (end = start + 1 day).
    
    Args:
        start_date: The start date of the charge period
        end_date: The end date of the charge period
        
    Returns:
        True if the charge period is valid, False otherwise
    """
    if start_date is None or end_date is None:
        return False
    
    # Calculate the expected end date (start + 1 day)
    expected_end = start_date + timedelta(days=1)
    
    # Check if the end date matches the expected end date
    return end_date.date() == expected_end.date()