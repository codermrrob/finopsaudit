"""
Decimal handler for precise cost field handling.

This module provides utilities for handling decimal values with high precision,
ensuring that cost fields are stored as DECIMAL128(38,32) without loss of precision.
"""

import decimal
import logging
from typing import Any, List, Optional, Union

import pyarrow as pa


class DecimalHandler:
    """
    Handler for decimal values with high precision.
    
    This class provides methods for converting values to Decimal128 with
    the specified precision and scale, ensuring that cost fields are stored
    without loss of precision.
    """
    
    # Default precision and scale for FOCUS cost fields
    DEFAULT_PRECISION = 38
    DEFAULT_SCALE = 32
    
    def __init__(self) -> None:
        """Initialize the decimal handler."""
        self.logger = logging.getLogger(__name__)
        
        # Configure decimal context for high precision
        decimal.getcontext().prec = self.DEFAULT_PRECISION
    
    def to_decimal128(
        self, value: Any, precision: int = DEFAULT_PRECISION, scale: int = DEFAULT_SCALE
    ) -> Optional[decimal.Decimal]:
        """
        Convert a value to Decimal128 with specified precision and scale.
        
        Args:
            value: The value to convert
            precision: The precision of the decimal (default: 38)
            scale: The scale of the decimal (default: 32)
            
        Returns:
            A PyArrow Decimal128 value, or None if the value is None or empty
            
        Raises:
            ValueError: If the value cannot be converted to Decimal128
        """
        if value is None or value == "":
            return None
        
        try:
            # Convert the value to a Python Decimal
            if isinstance(value, str):
                # Remove any currency symbols or commas
                value = value.replace("$", "").replace(",", "").strip()
            
            dec = decimal.Decimal(str(value))
            
            # Return the Python Decimal (PyArrow will convert it when needed)
            return dec
        
        except (decimal.InvalidOperation, ValueError) as e:
            self.logger.error(f"Failed to convert value '{value}' to Decimal128: {e}")
            raise ValueError(f"Invalid decimal value: {value}")
    
    def is_cost_field(self, field_name: str) -> bool:
        """
        Determine if a field is a cost field that requires decimal precision.
        
        Args:
            field_name: The name of the field to check
            
        Returns:
            True if the field is a cost field, False otherwise
        """
        field_name_lower = field_name.lower()
        
        # Exact matches for common cost field names
        exact_matches = ["cost", "price", "amount", "rate"]
        if field_name_lower in exact_matches:
            return True
            
        # Suffix matches for fields like "BilledCost", "UnitPrice"
        suffix_matches = ["cost", "price", "amount", "rate"]
        for suffix in suffix_matches:
            if field_name_lower.endswith(suffix):
                return True
                
        return False
    
    def get_decimal_type(
        self, precision: int = DEFAULT_PRECISION, scale: int = DEFAULT_SCALE
    ) -> pa.DataType:
        """
        Get a PyArrow decimal type with the specified precision and scale.
        
        Args:
            precision: The precision of the decimal (default: 38)
            scale: The scale of the decimal (default: 32)
            
        Returns:
            A PyArrow decimal128 type
        """
        return pa.decimal128(precision, scale)
