"""
Tag exploder for FOCUS ingestion service.

This module provides a component for exploding tag data into a flattened format
for the tags_index dataset.
"""

import logging
from typing import Optional

import pyarrow as pa


class TagExploder:
    """
    Exploder for tag data in FOCUS CSV files.
    
    This class provides methods for exploding tag data into a flattened format
    for the tags_index dataset.
    """
    
    def __init__(self) -> None:
        """Initialize the tag exploder."""
        self.logger = logging.getLogger(__name__)
    
    def explode_tags(
        self, batch: pa.RecordBatch
    ) -> Optional[pa.RecordBatch]:
        """
        Explode tag data from a batch into a flattened format.
        
        This method takes a batch with a 'Tags' column containing structured tag data
        and explodes it into a flattened format with one row per (resource_id, tag_key, tag_value)
        combination, preserving the partition columns.
        
        Args:
            batch: A PyArrow RecordBatch with a 'Tags' column
            
        Returns:
            A PyArrow RecordBatch with exploded tag data, or None if there are no tags
            
        Raises:
            ValueError: If required columns are missing
        """
        # Check if the batch has the required columns
        required_columns = [
            "ResourceId", "Tags", "year", "month", "day",
            "ResourceName", "RegionId", "RegionName", "SubAccountId", "SubAccountName",
            "ResourceType", "BillingAccountId", "ProviderName"  # Added new columns
        ]
        for column in required_columns:
            if column not in batch.schema.names:
                raise ValueError(f"Required column missing from batch: {column}")
        
        # Extract relevant columns
        resource_ids = batch.column("ResourceId").to_pylist()
        tags_column = batch.column("Tags")
        years = batch.column("year").to_pylist()
        months = batch.column("month").to_pylist()
        days = batch.column("day").to_pylist()
        resource_names = batch.column("ResourceName").to_pylist()
        region_ids = batch.column("RegionId").to_pylist()
        region_names = batch.column("RegionName").to_pylist()
        sub_account_ids = batch.column("SubAccountId").to_pylist()
        sub_account_names = batch.column("SubAccountName").to_pylist()
        resource_types = batch.column("ResourceType").to_pylist() # Added
        billing_account_ids = batch.column("BillingAccountId").to_pylist() # Added
        provider_names = batch.column("ProviderName").to_pylist() # Added
        
        # Explode tags
        exploded_resource_ids = []
        exploded_tag_keys = []
        exploded_tag_values = []
        exploded_years = []
        exploded_months = []
        exploded_days = []
        exploded_resource_names = []
        exploded_region_ids = []
        exploded_region_names = []
        exploded_sub_account_ids = []
        exploded_sub_account_names = []
        exploded_resource_types = []  # Added
        exploded_billing_account_ids = []  # Added
        exploded_provider_names = []  # Added
        
        # Process each row
        for i, (resource_id, year, month, day, resource_name, region_id, region_name, sub_account_id, sub_account_name, resource_type, billing_account_id, provider_name) in enumerate(
            zip(resource_ids, years, months, days, resource_names, region_ids, region_names, sub_account_ids, sub_account_names, resource_types, billing_account_ids, provider_names)
        ):
            # Get tags for this row
            try:
                # The tags column is a ListArray of StructArray with key and value fields
                tags_list = tags_column[i].as_py() # .as_py() on ListScalar returns a Python list of its elements
                
                if not tags_list:
                    continue
                
                # Add an entry for each tag
                for tag in tags_list:
                    exploded_resource_ids.append(resource_id)
                    exploded_tag_keys.append(tag["key"])
                    exploded_tag_values.append(tag["value"])
                    exploded_years.append(year)
                    exploded_months.append(month)
                    exploded_days.append(day)
                    exploded_resource_names.append(resource_name)
                    exploded_region_ids.append(region_id)
                    exploded_region_names.append(region_name)
                    exploded_sub_account_ids.append(sub_account_id)
                    exploded_sub_account_names.append(sub_account_name)
                    exploded_resource_types.append(resource_type) # Added
                    exploded_billing_account_ids.append(billing_account_id) # Added
                    exploded_provider_names.append(provider_name) # Added
            
            except Exception as e:
                self.logger.warning(f"Error processing tags for row {i}: {e}")
        
        # Return None if there are no exploded tags
        if not exploded_resource_ids:
            self.logger.debug("No tags to explode")
            return None
        
        # Create a new batch with the exploded data
        exploded_batch = pa.RecordBatch.from_arrays(
            [
                pa.array(exploded_resource_ids, type=pa.string()),
                pa.array(exploded_tag_keys, type=pa.string()),
                pa.array(exploded_tag_values, type=pa.string()),
                pa.array(exploded_resource_names, type=pa.string()),
                pa.array(exploded_region_ids, type=pa.string()),
                pa.array(exploded_region_names, type=pa.string()),
                pa.array(exploded_sub_account_ids, type=pa.string()),
                pa.array(exploded_sub_account_names, type=pa.string()),
                pa.array(exploded_resource_types, type=pa.string()),
                pa.array(exploded_billing_account_ids, type=pa.string()),
                pa.array(exploded_provider_names, type=pa.string()),
                pa.array(exploded_years, type=pa.int32()),
                pa.array(exploded_months, type=pa.int32()),
                pa.array(exploded_days, type=pa.int32()),
            ],
            names=["resource_id", "tag_key", "tag_value", 
                   "resource_name", "region_id", "region_name", "sub_account_id", "sub_account_name",
                   "resource_type", "billing_account_id", "provider_name", 
                   "year", "month", "day"]
        )
        
        return exploded_batch
    
    def get_tags_index_schema(self) -> pa.Schema:
        """
        Get the schema for the tags_index dataset.
        
        Returns:
            A PyArrow schema for the tags_index dataset
        """
        return pa.schema([
            pa.field("resource_id", pa.string()),
            pa.field("tag_key", pa.string()),
            pa.field("tag_value", pa.string()),
            pa.field("resource_name", pa.string()),
            pa.field("region_id", pa.string()),
            pa.field("region_name", pa.string()),
            pa.field("sub_account_id", pa.string()),
            pa.field("sub_account_name", pa.string()),
            pa.field("resource_type", pa.string()),  # Added
            pa.field("billing_account_id", pa.string()),  # Added
            pa.field("provider_name", pa.string()),  # Added
            pa.field("year", pa.int32()),
            pa.field("month", pa.int32()),
            pa.field("day", pa.int32()),
        ])
