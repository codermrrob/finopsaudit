"""
Unit tests for the tag parser.
"""

import unittest
import json

import pyarrow as pa

from focus_ingest.parser.tag_parser import TagParser


class TestTagParser(unittest.TestCase):
    """Test cases for the TagParser class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = TagParser()
    
    def test_parse_tags_json_valid(self):
        """Test that valid JSON tags are correctly parsed."""
        # Test valid JSON with tags
        tags_json = json.dumps({
            "Environment": "Production",
            "CostCenter": "IT",
            "Project": "Cloud Migration"
        })
        
        expected = [
            {"key": "Environment", "value": "Production"},
            {"key": "CostCenter", "value": "IT"},
            {"key": "Project", "value": "Cloud Migration"}
        ]
        
        result = self.parser.parse_tags_json(tags_json)
        
        # Sort both lists for comparison
        expected.sort(key=lambda x: x["key"])
        result.sort(key=lambda x: x["key"])
        
        self.assertEqual(result, expected)
    
    def test_parse_tags_json_empty(self):
        """Test that empty JSON is correctly parsed."""
        # Test empty JSON
        tags_json = "{}"
        result = self.parser.parse_tags_json(tags_json)
        self.assertEqual(result, [])
        
        # Test empty array
        tags_json = "[]"
        result = self.parser.parse_tags_json(tags_json)
        self.assertEqual(result, [])
    
    def test_parse_tags_json_none(self):
        """Test that None is correctly handled."""
        result = self.parser.parse_tags_json(None)
        self.assertEqual(result, [])
    
    def test_parse_tags_json_invalid(self):
        """Test that invalid JSON is correctly handled."""
        # Test invalid JSON
        tags_json = "not a json"
        result = self.parser.parse_tags_json(tags_json)
        self.assertEqual(result, [])
        
        # Test malformed JSON
        tags_json = "{\"key\": \"value\""  # Missing closing brace
        result = self.parser.parse_tags_json(tags_json)
        self.assertEqual(result, [])
    
    def test_get_tags_type(self):
        """Test that the tags type is correctly returned."""
        tags_type = self.parser.get_tags_type()
        
        # Check that it's a list type
        self.assertTrue(isinstance(tags_type, pa.ListType))
        
        # Check that the list contains structs
        struct_type = tags_type.value_type
        self.assertTrue(isinstance(struct_type, pa.StructType))
        
        # Check that the struct has key and value fields
        self.assertEqual(len(struct_type), 2)
        self.assertEqual(struct_type[0].name, "key")
        self.assertEqual(struct_type[1].name, "value")
        self.assertEqual(struct_type[0].type, pa.string())
        self.assertEqual(struct_type[1].type, pa.string())
    
    def test_convert_to_arrow_array(self):
        """Test that tags are correctly converted to Arrow arrays."""
        # Test with tags
        tags = [
            {"key": "Environment", "value": "Production"},
            {"key": "CostCenter", "value": "IT"},
            {"key": "Project", "value": "Cloud Migration"}
        ]
        
        result = self.parser.convert_to_arrow_array(tags)
        
        # Check that the result is an Arrow array
        self.assertTrue(isinstance(result, pa.Array))
        self.assertTrue(isinstance(result.type, pa.ListType))
        
        # Check that the array has the correct values
        pylist = result.to_pylist()
        self.assertEqual(len(pylist), 1)  # One batch of tags
        
        batch_tags = pylist[0]
        self.assertEqual(len(batch_tags), 3)  # Three tags
        
        # Convert batch_tags to a set of (key, value) tuples for easier comparison
        tag_tuples = {(tag["key"], tag["value"]) for tag in batch_tags}
        expected_tuples = {
            ("Environment", "Production"),
            ("CostCenter", "IT"),
            ("Project", "Cloud Migration")
        }
        
        self.assertEqual(tag_tuples, expected_tuples)
    
    def test_convert_to_arrow_array_empty(self):
        """Test that empty tags are correctly converted to Arrow arrays."""
        # Test with empty tags
        tags = []
        
        result = self.parser.convert_to_arrow_array(tags)
        
        # Check that the result is an Arrow array
        self.assertTrue(isinstance(result, pa.Array))
        self.assertTrue(isinstance(result.type, pa.ListType))
        
        # Check that the array has the correct values
        pylist = result.to_pylist()
        self.assertEqual(len(pylist), 1)  # One batch of tags
        
        batch_tags = pylist[0]
        self.assertEqual(len(batch_tags), 0)  # No tags


if __name__ == "__main__":
    unittest.main()
