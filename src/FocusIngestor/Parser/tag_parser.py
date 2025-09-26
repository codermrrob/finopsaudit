"""
Tag parser for FOCUS tag data.

This module provides utilities for parsing tag JSON data from FOCUS CSV files
and converting it to a structured format.
"""

import json
import logging
from typing import Dict, List

import pyarrow as pa


class TagParser:
    """
    Parser for tag data in FOCUS CSV files.
    
    This class provides methods for parsing tag JSON data and converting it
    to a structured format for storage in Parquet files.
    """
    
    def __init__(self) -> None:
        """Initialize the tag parser."""
        self.logger = logging.getLogger(__name__)
    
    def parse_tags_json(self, tags_json: str) -> List[Dict[str, str]]:
        if not isinstance(tags_json, str):
            self.logger.error(f"CRITICAL_TYPE_ERROR: parse_tags_json received non-string input. Type: {type(tags_json)}, Value (first 100 chars): '{str(tags_json)[:100]}'. This indicates an issue in the calling code (e.g., CSVReader._process_tag_fields).")
            # Attempt to handle common PyArrow scalars if they were mistakenly passed, though this is a workaround
            if hasattr(tags_json, 'as_py') and callable(getattr(tags_json, 'as_py')):
                self.logger.warning(f"Attempting to call .as_py() on non-string input of type {type(tags_json)}.")
                try:
                    py_value = tags_json.as_py()
                    if isinstance(py_value, str):
                        self.logger.warning(f".as_py() converted to string. Proceeding with this string: '{py_value[:100]}'")
                        tags_json = py_value # Use the converted string
                    elif isinstance(py_value, (list, dict)):
                        self.logger.warning(f".as_py() converted to {type(py_value)}. Attempting to json.dumps it.")
                        try:
                            tags_json = json.dumps(py_value)
                            self.logger.warning(f"Successfully json.dumped .as_py() value. Proceeding with: '{tags_json[:100]}'")
                        except Exception as dump_err:
                            self.logger.error(f"Failed to json.dumps the .as_py() value ({type(py_value)}): {dump_err}. Returning empty list.")
                            return []
                    else:
                        self.logger.error(f".as_py() resulted in unhandled type {type(py_value)}. Returning empty list.")
                        return []
                except Exception as e_as_py:
                    self.logger.error(f"Error calling .as_py() on non-string input or processing its result: {e_as_py}. Returning empty list.")
                    return []
            else:
                # If it's not a string and not a recognizable PyArrow scalar with .as_py()
                self.logger.error(f"Non-string input type {type(tags_json)} is not a recognized pa.Scalar. Returning empty list.")
                return []

        # Original parsing logic continues below
        """
        Parse a JSON string into a list of tag dictionaries.
        
        Args:
            tags_json: A JSON string containing tag data
            
        Returns:
            A list of dictionaries with 'key' and 'value' keys
            
        Raises:
            ValueError: If the JSON string is invalid
        """
        if not tags_json or tags_json == "" or tags_json.lower() == "null":
            return []
        
        try:
            # Parse the JSON string
            tags_data = json.loads(tags_json)
            
            # If it's already a list of dictionaries, ensure they have key/value structure
            if isinstance(tags_data, list):
                return [
                    {"key": item["key"], "value": item["value"]}
                    if "key" in item and "value" in item
                    else {"key": str(k), "value": str(v)}
                    for item in tags_data
                    for k, v in (item.items() if isinstance(item, dict) else [])
                ]
            
            # If it's a dictionary, convert to list of key/value dictionaries
            elif isinstance(tags_data, dict):
                return [
                    {"key": str(k), "value": str(v)}
                    for k, v in tags_data.items()
                ]
            
            else:
                self.logger.warning(f"Unexpected tags_json format: {tags_json}")
                return []
                
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse tags JSON: {e}, tags_json: {tags_json}")
            return []
        except Exception as e:
            self.logger.warning(f"Error processing tags JSON: {e}, tags_json: {tags_json}")
            return []
    
    def convert_to_arrow_array(self, all_rows_tags: List[List[Dict[str, str]]]) -> pa.Array:
        """
        Convert a list of lists of tag dictionaries (one list per row)
        to a PyArrow ListArray of StructArrays.
        
        Args:
            all_rows_tags: A list, where each element is a list of 
                           tag dictionaries (key/value pairs) for a row.
            
        Returns:
            A PyArrow ListArray where each element is a StructArray of tags.
        """
        # Define the struct type for individual tags, used for the inner values of the ListArray
        tag_struct_type = pa.struct([
            pa.field("key", pa.string()),
            pa.field("value", pa.string())
        ])

        offsets = [0]
        flat_tag_keys = []
        flat_tag_values = []
        current_offset = 0

        for row_tags in all_rows_tags:  # row_tags is List[Dict[str, str]]
            if row_tags:
                for tag_dict in row_tags: # tag_dict is Dict[str, str]
                    flat_tag_keys.append(tag_dict.get("key"))
                    flat_tag_values.append(tag_dict.get("value"))
                current_offset += len(row_tags)
            offsets.append(current_offset)

        keys_array = pa.array(flat_tag_keys, type=pa.string())
        values_array = pa.array(flat_tag_values, type=pa.string())

        # Create the StructArray for all tags flattened. This will be the 'values' of the ListArray.
        flat_struct_values = pa.StructArray.from_arrays([keys_array, values_array], fields=list(tag_struct_type))
        
        # Create the ListArray using offsets and the flattened StructArray values
        # The type of the ListArray is pa.list_(tag_struct_type)
        return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), flat_struct_values, type=self.get_tags_type())
    
    def get_tags_type(self) -> pa.DataType:
        """
        Get the PyArrow data type for tags.
        
        Returns:
            A PyArrow data type for tags
        """
        return pa.list_(
            pa.struct([
                pa.field("key", pa.string()),
                pa.field("value", pa.string())
            ])
        )
