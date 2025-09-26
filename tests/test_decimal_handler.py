"""
Unit tests for the decimal handler.
"""

import unittest
from decimal import Decimal

import pyarrow as pa
import numpy as np

from focus_ingest.parser.decimal_handler import DecimalHandler


class TestDecimalHandler(unittest.TestCase):
    """Test cases for the DecimalHandler class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.handler = DecimalHandler()
    
    def test_is_cost_field(self):
        """Test that cost fields are correctly identified."""
        # Test positive cases
        self.assertTrue(self.handler.is_cost_field("Cost"))
        self.assertTrue(self.handler.is_cost_field("ResourceCost"))
        self.assertTrue(self.handler.is_cost_field("TotalCost"))
        self.assertTrue(self.handler.is_cost_field("AmortizedCost"))
        self.assertTrue(self.handler.is_cost_field("Price"))
        self.assertTrue(self.handler.is_cost_field("ResourcePrice"))
        
        # Test negative cases
        self.assertFalse(self.handler.is_cost_field("ResourceId"))
        self.assertFalse(self.handler.is_cost_field("Description"))
        self.assertFalse(self.handler.is_cost_field("CostCenter"))  # Not a cost field
        self.assertFalse(self.handler.is_cost_field("PriceList"))   # Not a price field
    
    def test_get_decimal_type(self):
        """Test that the decimal type is correctly returned."""
        decimal_type = self.handler.get_decimal_type()
        self.assertEqual(decimal_type, pa.decimal128(38, 32))
    
    def test_to_decimal128_from_string(self):
        """Test that string values are correctly converted to DECIMAL128."""
        # Test valid string values
        self.assertEqual(
            self.handler.to_decimal128("123.45"),
            Decimal("123.45")
        )
        self.assertEqual(
            self.handler.to_decimal128("0.00000000001"),
            Decimal("0.00000000001")
        )
        self.assertEqual(
            self.handler.to_decimal128("-123.45"),
            Decimal("-123.45")
        )
        
        # Test string with more precision than we can handle
        self.assertEqual(
            self.handler.to_decimal128("0." + "1" * 40),
            Decimal("0." + "1" * 32 + "0" * 8)  # Truncated to 32 decimal places
        )
    
    def test_to_decimal128_from_float(self):
        """Test that float values are correctly converted to DECIMAL128."""
        # Test valid float values
        self.assertEqual(
            self.handler.to_decimal128(123.45),
            Decimal("123.45")
        )
        self.assertEqual(
            self.handler.to_decimal128(0.00000000001),
            Decimal("0.00000000001")
        )
        self.assertEqual(
            self.handler.to_decimal128(-123.45),
            Decimal("-123.45")
        )
    
    def test_to_decimal128_from_decimal(self):
        """Test that Decimal values are correctly handled."""
        # Test valid Decimal values
        self.assertEqual(
            self.handler.to_decimal128(Decimal("123.45")),
            Decimal("123.45")
        )
        self.assertEqual(
            self.handler.to_decimal128(Decimal("0.00000000001")),
            Decimal("0.00000000001")
        )
        
        # Test Decimal with more precision than we can handle
        self.assertEqual(
            self.handler.to_decimal128(Decimal("0." + "1" * 40)),
            Decimal("0." + "1" * 32 + "0" * 8)  # Truncated to 32 decimal places
        )
    
    def test_to_decimal128_from_none(self):
        """Test that None values are correctly handled."""
        self.assertIsNone(self.handler.to_decimal128(None))
    
    def test_to_decimal128_invalid_input(self):
        """Test that invalid inputs raise appropriate exceptions."""
        with self.assertRaises(ValueError):
            self.handler.to_decimal128("not a number")
        
        with self.assertRaises(ValueError):
            self.handler.to_decimal128("123.45.67")
        
        with self.assertRaises(TypeError):
            self.handler.to_decimal128(object())  # Unsupported type


if __name__ == "__main__":
    unittest.main()
