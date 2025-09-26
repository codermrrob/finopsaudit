"""
Unit tests for the tag exploder.
"""

import unittest

import pyarrow as pa
import numpy as np

from focus_ingest.writer.tag_exploder import TagExploder
from focus_ingest.parser.tag_parser import TagParser


class TestTagExploder(unittest.TestCase):
    """Test cases for the TagExploder class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.exploder = TagExploder()
        self.tag_parser = TagParser()
    
    def create_test_batch(self):
        """Create a test batch with tag data."""
        # Create resource IDs
        resource_ids = pa.array(["resource1", "resource2", "resource3"])
        
        # Create tags
        tag_data = [
            [{"key": "Environment", "value": "Production"}, {"key": "CostCenter", "value": "IT"}],
            [{"key": "Environment", "value": "Development"}, {"key": "Project", "value": "Migration"}],
            []  # Empty tags
        ]
        
        tags_arrays = []
        for tags in tag_data:
            tags_arrays.append(self.tag_parser.convert_to_arrow_array(tags))
        
        # Create a struct array for tags
        tags = pa.StructArray.from_arrays(
            [pa.array(resource_ids), pa.array(tags_arrays)],
            ["ResourceId", "Tags"]
        )
        
        # Create partition columns
        year = pa.array([2025, 2025, 2025], type=pa.int32())
        month = pa.array([1, 1, 2], type=pa.int32())
        day = pa.array([1, 1, 1], type=pa.int32())
        
        # Create the batch
        return pa.RecordBatch.from_arrays(
            [resource_ids, tags_arrays[0], year, month, day],
            names=["ResourceId", "Tags", "year", "month", "day"]
        )
    
    def test_explode_tags(self):
        """Test that tags are correctly exploded."""
        # Create a batch with tag data
        batch = self.create_test_batch()
        
        # Explode tags
        exploded_batch = self.exploder.explode_tags(batch)
        
        # Check that the exploded batch has the expected columns
        expected_columns = ["resource_id", "tag_key", "tag_value", "year", "month", "day"]
        for column in expected_columns:
            self.assertIn(column, exploded_batch.schema.names)
        
        # Check that the exploded batch has the expected number of rows
        self.assertEqual(len(exploded_batch), 4)  # 2 tags for resource1, 2 for resource2, 0 for resource3
        
        # Check the values in the exploded batch
        resource_ids = exploded_batch.column("resource_id").to_pylist()
        tag_keys = exploded_batch.column("tag_key").to_pylist()
        tag_values = exploded_batch.column("tag_value").to_pylist()
        
        # Expected values
        expected_resource_ids = ["resource1", "resource1", "resource2", "resource2"]
        expected_tag_keys = ["Environment", "CostCenter", "Environment", "Project"]
        expected_tag_values = ["Production", "IT", "Development", "Migration"]
        
        # Check that the values match
        self.assertEqual(set(resource_ids), set(expected_resource_ids))
        
        # Create a set of (resource_id, tag_key, tag_value) tuples for easier comparison
        tag_tuples = set(zip(resource_ids, tag_keys, tag_values))
        expected_tuples = set(zip(expected_resource_ids, expected_tag_keys, expected_tag_values))
        
        self.assertEqual(tag_tuples, expected_tuples)
    
    def test_explode_tags_no_tags(self):
        """Test that exploding a batch with no tags returns None."""
        # Create a batch with no tags
        resource_ids = pa.array(["resource1", "resource2"])
        empty_tags = self.tag_parser.convert_to_arrow_array([])
        
        # Repeat the empty tags for each resource
        tags = pa.array([empty_tags, empty_tags])
        
        # Create partition columns
        year = pa.array([2025, 2025], type=pa.int32())
        month = pa.array([1, 1], type=pa.int32())
        day = pa.array([1, 1], type=pa.int32())
        
        # Create the batch
        batch = pa.RecordBatch.from_arrays(
            [resource_ids, tags, year, month, day],
            names=["ResourceId", "Tags", "year", "month", "day"]
        )
        
        # Explode tags
        exploded_batch = self.exploder.explode_tags(batch)
        
        # Check that the result is None
        self.assertIsNone(exploded_batch)
    
    def test_get_tags_index_schema(self):
        """Test that the tags index schema is correctly returned."""
        schema = self.exploder.get_tags_index_schema()
        
        # Check that the schema has the expected fields
        expected_fields = ["resource_id", "tag_key", "tag_value", "year", "month", "day"]
        for field_name in expected_fields:
            self.assertIn(field_name, schema.names)
        
        # Check that the fields have the expected types
        self.assertEqual(schema.field("resource_id").type, pa.string())
        self.assertEqual(schema.field("tag_key").type, pa.string())
        self.assertEqual(schema.field("tag_value").type, pa.string())
        self.assertEqual(schema.field("year").type, pa.int32())
        self.assertEqual(schema.field("month").type, pa.int32())
        self.assertEqual(schema.field("day").type, pa.int32())


if __name__ == "__main__":
    unittest.main()
