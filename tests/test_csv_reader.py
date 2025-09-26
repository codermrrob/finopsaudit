"""
Unit tests for the CSV reader.
"""

import unittest
import tempfile
import os
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pandas as pd

from ..Parser.csv_reader import CSVReader
from ..Parser.decimal_handler import DecimalHandler
from ..Parser.tag_parser import TagParser
from ..FileSystem.local import LocalFileSystem


class TestCSVReader(unittest.TestCase):
    """Test cases for the CSVReader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.fs = LocalFileSystem()
        self.decimal_handler = DecimalHandler()
        self.tag_parser = TagParser()
        self.reader = CSVReader(self.fs, self.decimal_handler, self.tag_parser, batch_size=100)
        
        # Create a test CSV file
        self.create_test_csv()
    
    def tearDown(self):
        """Tear down test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def create_test_csv(self):
        """Create a test CSV file with FOCUS-like data."""
        # Create a DataFrame with FOCUS-like columns
        data = {
            "ResourceId": ["resource1", "resource2", "resource3"],
            "ChargePeriodStart": ["2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"],
            "ChargePeriodEnd": ["2025-01-02T00:00:00Z", "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z"],
            "Cost": ["123.45", "678.90", "0.12345"],
            "ResourceCost": ["100.00", "600.00", "0.10000"],
            "Quantity": ["1", "2", "3"],
            "Tags": [
                '{"Environment": "Production", "CostCenter": "IT"}',
                '{"Environment": "Development", "Project": "Migration"}',
                '{}'
            ],
            "x_VendorField": ["vendor1", "vendor2", "vendor3"]
        }
        
        df = pd.DataFrame(data)
        
        # Write the DataFrame to a CSV file
        self.csv_path = os.path.join(self.temp_dir, "test.csv")
        df.to_csv(self.csv_path, index=False)
        
        # Store the URI for the file
        self.csv_uri = f"file://{Path(self.csv_path).as_posix()}"
    
    def test_read_batches(self):
        """Test that batches are correctly read from a CSV file."""
        # Read batches
        batches = list(self.reader.read_batches(self.csv_uri))
        
        # Check that there is at least one batch
        self.assertGreater(len(batches), 0)
        
        # Check the first batch
        batch, metadata = batches[0]
        
        # Check that the batch has the expected columns
        expected_columns = [
            "ResourceId", "ChargePeriodStart", "ChargePeriodEnd", 
            "Cost", "ResourceCost", "Quantity", "Tags", "x_VendorField",
            "year", "month", "day"
        ]
        
        for column in expected_columns:
            self.assertIn(column, batch.schema.names)
        
        # Check that the batch has the expected number of rows
        self.assertEqual(len(batch), 3)
        
        # Check that partition info is in metadata
        self.assertIn("partition_info", metadata)
        self.assertIn("row_count", metadata)
        self.assertEqual(metadata["row_count"], 3)
    
    def test_cost_field_conversion(self):
        """Test that cost fields are correctly converted to DECIMAL128."""
        # Read batches
        batches = list(self.reader.read_batches(self.csv_uri))
        batch, _ = batches[0]
        
        # Check that cost fields have the correct type
        cost_fields = ["Cost", "ResourceCost"]
        for field in cost_fields:
            self.assertEqual(
                batch.schema.field(field).type,
                self.decimal_handler.get_decimal_type()
            )
        
        # Check that a non-cost field doesn't have the decimal type
        self.assertNotEqual(
            batch.schema.field("Quantity").type,
            self.decimal_handler.get_decimal_type()
        )
    
    def test_tag_field_conversion(self):
        """Test that tag fields are correctly converted to structured format."""
        # Read batches
        batches = list(self.reader.read_batches(self.csv_uri))
        batch, _ = batches[0]
        
        # Check that the Tags column has the expected type
        self.assertEqual(
            batch.schema.field("Tags").type,
            self.tag_parser.get_tags_type()
        )
    
    def test_partition_columns(self):
        """Test that partition columns are correctly added."""
        # Read batches
        batches = list(self.reader.read_batches(self.csv_uri))
        batch, _ = batches[0]
        
        # Check that partition columns exist and have the correct types
        self.assertIn("year", batch.schema.names)
        self.assertIn("month", batch.schema.names)
        self.assertIn("day", batch.schema.names)
        
        self.assertEqual(batch.schema.field("year").type, pa.int32())
        self.assertEqual(batch.schema.field("month").type, pa.int32())
        self.assertEqual(batch.schema.field("day").type, pa.int32())
        
        # Check that the partition values are correct
        years = batch.column("year").to_pylist()
        months = batch.column("month").to_pylist()
        days = batch.column("day").to_pylist()
        
        self.assertEqual(years, [2025, 2025, 2025])
        self.assertEqual(months, [1, 1, 1])
        self.assertEqual(days, [1, 1, 2])
    
    def test_missing_required_columns(self):
        """Test that an error is raised if required columns are missing."""
        # Create a CSV file without required columns
        data = {
            "Column1": ["value1", "value2"],
            "Column2": ["value1", "value2"]
        }
        
        df = pd.DataFrame(data)
        
        csv_path = os.path.join(self.temp_dir, "missing_columns.csv")
        df.to_csv(csv_path, index=False)
        
        csv_uri = f"file://{Path(csv_path).as_posix()}"
        
        # Try to read batches from the file
        with self.assertRaises(ValueError):
            list(self.reader.read_batches(csv_uri))


if __name__ == "__main__":
    unittest.main()
