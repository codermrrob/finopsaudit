"""
Unit tests for date utilities.
"""

import unittest
from datetime import datetime

from focus_ingest.utils.date import (
    parse_date,
    extract_partition_info,
    validate_charge_period,
    get_partition_path
)


class TestDateUtils(unittest.TestCase):
    """Test cases for date utility functions."""
    
    def test_parse_date(self):
        """Test that date strings are correctly parsed."""
        # Test valid ISO format
        result = parse_date("2025-01-01T00:00:00Z")
        self.assertEqual(result, datetime(2025, 1, 1, 0, 0, 0))
        
        # Test date without time
        result = parse_date("2025-01-01")
        self.assertEqual(result, datetime(2025, 1, 1, 0, 0, 0))
        
        # Test with milliseconds
        result = parse_date("2025-01-01T00:00:00.123Z")
        self.assertEqual(result, datetime(2025, 1, 1, 0, 0, 0, 123000))
        
        # Test None input
        result = parse_date(None)
        self.assertIsNone(result)
        
        # Test empty string
        result = parse_date("")
        self.assertIsNone(result)
        
        # Test invalid format
        with self.assertRaises(ValueError):
            parse_date("not a date")
    
    def test_extract_partition_info(self):
        """Test that partition info is correctly extracted from dates."""
        # Test with dates
        dates = [
            datetime(2025, 1, 1),
            datetime(2025, 2, 15),
            datetime(2025, 3, 31)
        ]
        
        expected = [
            (2025, 1, 1),
            (2025, 2, 15),
            (2025, 3, 31)
        ]
        
        result = extract_partition_info(dates)
        self.assertEqual(result, expected)
        
        # Test with None values
        dates = [None, datetime(2025, 1, 1), None]
        
        expected = [
            (1970, 1, 1),  # Default for None
            (2025, 1, 1),
            (1970, 1, 1)   # Default for None
        ]
        
        result = extract_partition_info(dates)
        self.assertEqual(result, expected)
    
    def test_validate_charge_period(self):
        """Test that charge periods are correctly validated."""
        # Test valid charge period (end = start + 1 day)
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 2)
        self.assertTrue(validate_charge_period(start, end))
        
        # Test invalid charge period (end != start + 1 day)
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 3)
        self.assertFalse(validate_charge_period(start, end))
        
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 1)
        self.assertFalse(validate_charge_period(start, end))
        
        # Test with None values
        self.assertFalse(validate_charge_period(None, end))
        self.assertFalse(validate_charge_period(start, None))
        self.assertFalse(validate_charge_period(None, None))
    
    def test_get_partition_path(self):
        """Test that partition paths are correctly generated."""
        # Test with various inputs
        result = get_partition_path("/data", "tenant", "costs_raw", 2025, 1, 1)
        self.assertEqual(result, "/data/tenant/parquet/costs_raw/year=2025/month=01/day=01")
        
        result = get_partition_path("/data", "tenant", "tags_index", 2025, 12, 31)
        self.assertEqual(result, "/data/tenant/parquet/tags_index/year=2025/month=12/day=31")
        
        # Test with non-standard values
        result = get_partition_path("/data", "tenant", "costs_raw", 1970, 1, 1)
        self.assertEqual(result, "/data/tenant/parquet/costs_raw/year=1970/month=01/day=01")
        
        # Test with single-digit month and day
        result = get_partition_path("/data", "tenant", "costs_raw", 2025, 1, 1)
        self.assertEqual(result, "/data/tenant/parquet/costs_raw/year=2025/month=01/day=01")


if __name__ == "__main__":
    unittest.main()
