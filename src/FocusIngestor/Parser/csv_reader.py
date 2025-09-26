"""
CSV reader for FOCUS CSV files.

This module provides a streaming CSV reader for FOCUS CSV files that handles
large files by processing them in batches.
"""

import logging
import json
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Any, Union
from datetime import datetime, timezone
import re

import pyarrow as pa
import pyarrow.csv as csv

from FileSystem.base import FileSystem
from .decimal_handler import DecimalHandler
from .tag_parser import TagParser
from ..Utils.date import extract_partition_info
from ..FocusSpecification.schema_manager import FocusSchemaManager


class CSVReader:
    """
    Streaming reader for FOCUS CSV files.
    
    This class provides methods for reading FOCUS CSV files in a streaming manner,
    converting cost fields to DECIMAL128(38,32) and parsing tag data.
    """
    
    def __init__(
        self, 
        fs: FileSystem, 
        decimal_handler: Optional[DecimalHandler] = None,
        tag_parser: Optional[TagParser] = None,
        batch_size: int = 100000
    ) -> None:
        """
        Initialize the CSV reader.
        
        Args:
            fs: The filesystem to use for reading files
            decimal_handler: Handler for decimal precision (default: new instance)
            tag_parser: Parser for tag data (default: new instance)
            batch_size: The number of rows to read in each batch (default: 200000)
        """
        self.fs = fs
        self.decimal_handler = decimal_handler or DecimalHandler()
        self.tag_parser = tag_parser or TagParser()
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)

        # Initialize the schema manager
        self.schema_manager = FocusSchemaManager()
        self.final_schema = self.schema_manager.schema
        self.feature_level_map = self.schema_manager.feature_level_map
    
    def read_batches(self, uri: str) -> Iterator[Tuple[pa.RecordBatch, Dict[str, Any]]]:
        """
        Read a CSV file in batches.
        
        This method reads a FOCUS CSV file in batches, converting cost fields
        to DECIMAL128(38,32) and parsing tag data. It yields tuples of
        (processed_batch, metadata) where metadata contains information about
        the batch, such as date partitioning information.
        
        Args:
            uri: The URI of the CSV file to read
            
        Yields:
            Tuples of (processed_batch, metadata) where processed_batch is a
            PyArrow RecordBatch and metadata is a dictionary with metadata about
            the batch
            
        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If the file cannot be opened
            ValueError: If the file does not contain required columns
        """
        self.logger.info(f"Reading CSV file: {uri}")
        
        try:
            with self.fs.open_input_stream(uri) as file:
                # Configure CSV reader options
                read_options = csv.ReadOptions(block_size=self.batch_size * 1024)  # Block size in bytes
                parse_options = csv.ParseOptions(delimiter=',')
                # Ensure date columns are read as strings, so our Python parser can handle them
                convert_options = csv.ConvertOptions(
                    column_types={
                        "ChargePeriodStart": pa.string(),
                        "ChargePeriodEnd": pa.string(),
                        "Tags": pa.string()  # Ensure Tags column is read as string
                    }
                )
                
                # Create CSV reader
                reader = csv.open_csv(
                    file, 
                    read_options=read_options, 
                    parse_options=parse_options, 
                    convert_options=convert_options
                )
                
                # Check for required columns
                schema = reader.schema
                self._validate_schema(schema)
                
                # Read and process batches
                batch_count = 0
                for batch in reader:
                    batch_count += 1
                    self.logger.debug(f"Processing batch {batch_count} with {len(batch)} rows")
                    
                    try:
                        # Process the batch
                        processed_batch, metadata = self._process_batch(batch)
                        
                        # Yield the processed batch and metadata
                        yield processed_batch, metadata
                    
                    except Exception as e:
                        self.logger.error(f"Error processing batch {batch_count}: {e}")
                        # Continue with next batch
                
                self.logger.info(f"Finished reading {batch_count} batches from {uri}")
        
        except Exception as e:
            self.logger.error(f"Error reading CSV file {uri}: {e}")
            raise
    
    def _validate_schema(self, schema: pa.Schema) -> None:
        """
        Validate that the CSV schema contains required columns.
        
        Args:
            schema: The PyArrow schema of the CSV file
            
        Raises:
            ValueError: If the schema does not contain required columns
        """
        # Use the schema manager to validate mandatory columns
        mandatory_columns = self.schema_manager.mandatory_columns
        missing_mandatory = [col for col in mandatory_columns if col not in schema.names]

        if missing_mandatory:
            error_msg = f"CSV schema missing mandatory FOCUS columns: {missing_mandatory}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
    
    def _process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, Dict[str, Any]]:
        """
        Process a batch of CSV data.
        
        This method processes a batch of CSV data, converting cost fields to
        DECIMAL128(38,32), parsing tag data, and extracting date partitioning
        information.
        
        Args:
            batch: A PyArrow RecordBatch containing CSV data
            
        Returns:
            A tuple of (processed_batch, metadata) where processed_batch is a
            PyArrow RecordBatch and metadata is a dictionary with metadata about
            the batch
            
        Raises:
            ValueError: If required columns are missing or invalid
        """
        # First, apply the final schema to the batch. This is the most critical step.
        # It casts all columns read as strings (like decimals and dates) to their
        # correct final types based on the YAML spec. This prevents any downstream
        # logic from incorrectly re-inferring types.
        batch = self._apply_final_schema(batch)

        # Extract and validate date fields for partitioning.
        # The columns are now proper timestamp types, so we can read them directly.
        try:
            start_dates_col = batch.column("ChargePeriodStart").to_pylist()

            # Extract partition info
            partition_info = extract_partition_info(start_dates_col)
            
            # Add partition columns to the batch
            batch = self._add_partition_columns(batch, partition_info)

            # Prepare metadata
            metadata = {
                "partition_info": partition_info,
                "row_count": len(batch)
            }
            
            return batch, metadata
        
        except Exception as e:
            self.logger.error(f"Error during batch processing after schema application: {e}", exc_info=True)
            raise
    
    def _parse_date(self, date_val: Optional[Union[str, datetime]]) -> Optional[datetime]:
        # self.logger.debug(f"Attempting to parse date value: '{date_val}' (type: {type(date_val)})")

        if isinstance(date_val, datetime):
            # self.logger.debug(f"Input is already a datetime object: {date_val}. Ensuring UTC.")
            if date_val.tzinfo is None:
                return date_val.replace(tzinfo=timezone.utc) # Localize naive datetime to UTC
            elif date_val.tzinfo != timezone.utc:
                return date_val.astimezone(timezone.utc) # Convert existing timezone to UTC
            return date_val # Already a UTC datetime object

        # If not a datetime, check if it's a string for parsing
        if not isinstance(date_val, str):
            if date_val is None:
                self.logger.debug("Date value is None, returning None.")
            else:
                self.logger.warning(
                    f"Date value '{date_val}' is not a string or datetime (type: {type(date_val)}), cannot parse. Returning None."
                )
            return None

        # Now we know date_val is a string, assign to date_str for existing logic
        date_str = date_val
        if not date_str.strip(): # Handles "" and "   "
            self.logger.debug(f"Date string '{date_str}' is empty or whitespace, returning None.")
            return None
            
        original_date_str = date_str # original_date_str is now guaranteed to be a non-empty string

        try:
            # Case 1: ISO format ending with 'Z'
            if date_str.endswith('Z'):
                # self.logger.debug(f"Parsing '{date_str}' as ISO format with Z.")
                try:
                    # datetime.fromisoformat handles 'Z' correctly as UTC
                    dt = datetime.fromisoformat(date_str)
                    # self.logger.debug(f"Successfully parsed '{date_str}' with Z to {dt} using fromisoformat.")
                    return dt
                except ValueError as ve_z:
                    self.logger.debug(f"fromisoformat failed for '{date_str}' with Z: {ve_z}. Trying Z replacement.")
                    # Fallback: replace 'Z' with '+00:00' if direct parsing fails (e.g., non-compliant string)
                    try:
                        dt_str_plus_offset = date_str[:-1] + "+00:00"
                        dt = datetime.fromisoformat(dt_str_plus_offset)
                        # self.logger.debug(f"Successfully parsed '{date_str}' (as '{dt_str_plus_offset}') to {dt}.")
                        return dt # dt is already offset-aware
                    except ValueError as ve_z_fallback:
                        self.logger.debug(f"Fallback Z replacement parsing for '{date_str}' also failed: {ve_z_fallback}. Will fall through.")
                        # Fall through to other parsing methods if this also fails

            # Case 2: Contains 'UTC' (case-insensitive) as a potential timezone designator at the end
            temp_date_str_upper = date_str.upper()
            utc_suffix_to_strip = None
            if temp_date_str_upper.endswith(' UTC'):
                utc_suffix_to_strip = date_str[-(len(' UTC')):] # Capture exact case for stripping
            elif temp_date_str_upper.endswith('UTC'):
                utc_suffix_to_strip = date_str[-(len('UTC')):] # Capture exact case
            
            if utc_suffix_to_strip:
                self.logger.debug(f"Parsing '{date_str}' potentially ending with '{utc_suffix_to_strip}'.")
                timestamp_part = date_str[:-len(utc_suffix_to_strip)].strip()
                try:
                    dt_naive = datetime.fromisoformat(timestamp_part)
                    dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                    self.logger.debug(f"Successfully parsed timestamp part '{timestamp_part}' from '{date_str}' to {dt_utc}.")
                    return dt_utc
                except ValueError as ve_utc:
                    self.logger.debug(f"Failed to parse timestamp part '{timestamp_part}' from '{date_str}' using fromisoformat: {ve_utc}. Will fall through.")
                    # Fall through if fromisoformat fails for the timestamp_part

            # Case 3: Try standard ISO format (could be naive or already have an offset)
            self.logger.debug(f"Trying to parse '{date_str}' directly with fromisoformat as generic ISO.")
            try:
                dt = datetime.fromisoformat(date_str)
                if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                    dt_utc = dt.replace(tzinfo=timezone.utc)
                    self.logger.debug(f"Parsed '{date_str}' as naive ISO, converted to UTC: {dt_utc}.")
                    return dt_utc
                else:
                    dt_utc = dt.astimezone(timezone.utc) # Convert to UTC if it has a different offset
                    self.logger.debug(f"Parsed '{date_str}' as offset-aware ISO, ensured UTC: {dt_utc}.")
                    return dt_utc
            except ValueError as ve_iso:
                self.logger.debug(f"Failed to parse '{date_str}' directly as generic ISO with fromisoformat: {ve_iso}. Will fall through.")
                # Fall through
                
            # Case 4: Try to parse as YYYY-MM-DD (assuming UTC for date-only strings)
            self.logger.debug(f"Trying to parse '{date_str}' as YYYY-MM-DD.")
            # Use re.fullmatch to ensure the entire string is just the date
            match = re.fullmatch(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str.strip())
            if match:
                year, month, day = map(int, match.groups())
                dt_utc = datetime(year, month, day, tzinfo=timezone.utc)
                self.logger.debug(f"Parsed '{date_str}' as YYYY-MM-DD to {dt_utc}.")
                return dt_utc
            else:
                self.logger.debug(f"'{date_str}' did not match YYYY-MM-DD format.")

            # Default: If all parsing attempts fail
            self.logger.warning(f"Couldn't parse date '{original_date_str}' with any known method. Defaulting to epoch (1970-01-01 UTC).")
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
            
        except Exception as e:
            # Catch-all for any unexpected error during the parsing logic itself
            self.logger.error(f"Unexpected critical error during parsing of date string '{original_date_str}': {e}", exc_info=True)
            self.logger.warning(f"Due to unexpected critical error, defaulting to epoch for '{original_date_str}'.")
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    def _extract_dates(self, batch: pa.RecordBatch, column_name: str) -> List[datetime]:
        """
        Extract and parse dates from a column.
        
        Args:
            batch: A PyArrow RecordBatch
            column_name: The name of the column containing dates
            
        Returns:
            A list of datetime objects
            
        Raises:
            ValueError: If the column does not exist or contains invalid dates
        """
        if column_name not in batch.schema.names:
            # This check is important because the input batch might not have all columns
            # before the final schema is applied.
            self.logger.debug(f"Date column '{column_name}' not found in this batch, returning empty list.")
            return []
        
        # Get the column as a Python list
        date_strings = batch.column(column_name).to_pylist()
        
        # Parse dates using our internal method
        dates = []
        for i, date_str in enumerate(date_strings):
            date = self._parse_date(date_str)
            dates.append(date)
        
        return dates
    
    def _add_partition_columns( self, batch: pa.RecordBatch, partition_info: List[Tuple[int, int, int]]) -> pa.RecordBatch:
        """
        Add partition columns (year, month, day) to a batch.
        
        Args:
            batch: A PyArrow RecordBatch
            partition_info: A list of (year, month, day) tuples
            
        Returns:
            A PyArrow RecordBatch with partition columns added
        """
        # Create arrays for year, month, day, ensuring types match the schema
        year_array = pa.array([p[0] for p in partition_info], type=pa.int64())
        month_array = pa.array([str(p[1]) for p in partition_info], type=pa.string())
        day_array = pa.array([str(p[2]) for p in partition_info], type=pa.string())
        
        # Add arrays to the batch
        return batch.append_column("year", year_array).append_column("month", month_array).append_column("day", day_array)
    
    def _apply_final_schema(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Process all fields in a batch, ensuring all output columns conform to self.final_schema.

        This method iterates through the final schema (derived from YAML) and casts
        each column in the batch to its definitive data type. If a column is missing,
        it's created as a null column. This is the single source of truth for typing.

        Args:
            batch: A PyArrow RecordBatch, typically with many columns as strings.

        Returns:
            A PyArrow RecordBatch strictly conforming to self.final_schema.
        """
        if not hasattr(self, 'final_schema') or not self.final_schema:
            self.logger.error("final_schema is not set in CSVReader. Cannot process batch correctly.")
            return batch

        processed_arrays = []

        for field_in_final_schema in self.final_schema:
            col_name = field_in_final_schema.name
            target_type = field_in_final_schema.type
            current_array = None

            try:
                current_array = batch.column(col_name)
            except KeyError:
                feature_level = self.feature_level_map.get(col_name, 'N/A')
                # Skip creating partition columns here as they are added later
                if col_name in ["year", "month", "day"]:
                    continue
                self.logger.warning(
                    f"Column '{col_name}' (feature level: {feature_level}) from final_schema not found in current batch. "
                    f"Creating a null column of type {target_type}."
                )
                processed_arrays.append(pa.nulls(len(batch), type=target_type))
                continue


            # Handle all decimal conversions uniformly, driven by the final schema type
            if pa.types.is_decimal(target_type):
                try:
                    py_list = current_array.to_pylist()
                    decimal_values = [self.decimal_handler.to_decimal128(v) for v in py_list]
                    casted_array = pa.array(decimal_values, type=target_type)
                    processed_arrays.append(casted_array)
                    self.logger.debug(f"Converted column '{col_name}' to {target_type}")
                except Exception as e:
                    self.logger.error(
                        f"Error converting column '{col_name}' to {target_type}: {e}. "
                        f"Using nulls."
                    )
                    processed_arrays.append(pa.nulls(len(current_array), type=target_type))

            # Special override for PricingQuantity to match final DuckDB schema (BIGINT)
            elif col_name == 'PricingQuantity':
                try:
                    # Cast to int64, allowing truncation from float/decimal if necessary
                    casted_array = current_array.cast(pa.int64(), safe=False)
                    processed_arrays.append(casted_array)
                    self.logger.debug(f"Forced column '{col_name}' to {pa.int64()} to match DuckDB schema.")
                except Exception as e:
                    self.logger.error(
                        f"Error forcing column '{col_name}' to {pa.int64()}: {e}. "
                        f"Using nulls."
                    )
                    processed_arrays.append(pa.nulls(len(current_array), type=pa.int64()))

            # Handle timestamp conversions, driven by the final schema type
            elif pa.types.is_timestamp(target_type):
                try:
                    py_list = current_array.to_pylist()
                    date_values = [self._parse_date(v) for v in py_list]
                    casted_array = pa.array(date_values, type=target_type)
                    processed_arrays.append(casted_array)
                    self.logger.debug(f"Converted column '{col_name}' to {target_type}")
                except Exception as e:
                    self.logger.error(
                        f"Error converting column '{col_name}' to {target_type}: {e}. "
                        f"Using nulls."
                    )
                    processed_arrays.append(pa.nulls(len(current_array), type=target_type))
            else: # For all other types (string, etc.), just ensure it conforms
                try:
                    if current_array.type.equals(target_type):
                        processed_arrays.append(current_array)
                    else:
                        casted_array = current_array.cast(target_type, safe=False)
                        processed_arrays.append(casted_array)
                        self.logger.debug(f"Casted column '{col_name}' from {current_array.type} to {target_type}")
                except Exception as e:
                    self.logger.error(
                        f"Error casting column '{col_name}' from {current_array.type} to {target_type}: {e}. "
                        f"Using nulls."
                    )
                    processed_arrays.append(pa.nulls(len(current_array), type=target_type))
        
        # Rebuild the schema, excluding partition columns that will be added later.
        final_fields_without_partitions = [f for f in self.final_schema if f.name not in ["year", "month", "day"]]
        return pa.RecordBatch.from_arrays(processed_arrays, schema=pa.schema(final_fields_without_partitions))
    
    def _process_tag_fields(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Process tag fields in a batch.
        
        This method identifies the Tags column in the batch and converts it to a
        structured format.
        
        Args:
            batch: A PyArrow RecordBatch
            
        Returns:
            A PyArrow RecordBatch with tag fields converted to structured format
        """
        arrays = list(batch.columns)
        names = list(batch.schema.names)
        fields = [batch.schema.field(i) for i in range(len(names))]
        
        # Check if the Tags column exists
        if "Tags" in names:
            try:
                tags_index = names.index("Tags")
                tags_column_data = batch.column(tags_index)
                tags_strings = [] # This will hold a string (JSON or empty) for each row

                if isinstance(tags_column_data, pa.StringScalar):
                    # If the entire column is a single string scalar (e.g., batch has 1 row and Tags is string)
                    tag_content = tags_column_data.as_py() if tags_column_data.is_valid else ""
                    tags_strings = [tag_content] * len(batch) # Replicate for all rows in batch
                
                elif isinstance(tags_column_data, (pa.Array, pa.ChunkedArray)):
                    current_row_in_batch_tags_processing = 0
                    arr_iterator = tags_column_data.chunks if isinstance(tags_column_data, pa.ChunkedArray) else [tags_column_data]
                    for arr_chunk in arr_iterator:
                        for i, scalar_val in enumerate(arr_chunk):
                            current_tag_str = ""  # Default for this scalar/row
                            if scalar_val is not None and scalar_val.is_valid:
                                if isinstance(scalar_val, pa.ListScalar):
                                    python_list_of_dicts = []
                                    try:
                                        for item_struct_scalar in scalar_val.value: # .value is the inner Array
                                            if item_struct_scalar is not None and item_struct_scalar.is_valid:
                                                py_item = item_struct_scalar.as_py()
                                                if isinstance(py_item, dict):
                                                    python_list_of_dicts.append(py_item)
                                                else:
                                                    self.logger.warning(f"Tag item in ListScalar (batch row ~{current_row_in_batch_tags_processing + i}) not a dict: {type(py_item)}")
                                        current_tag_str = json.dumps(python_list_of_dicts)
                                    except Exception as list_scalar_err:
                                        self.logger.warning(f"Error processing ListScalar for tags (batch row ~{current_row_in_batch_tags_processing + i}): {list_scalar_err}. Defaulting to empty JSON list.")
                                        current_tag_str = json.dumps([])
                                elif isinstance(scalar_val, pa.StructScalar):
                                    try:
                                        py_dict = scalar_val.as_py()
                                        current_tag_str = json.dumps([py_dict]) # Wrap single struct in a list
                                    except Exception as struct_scalar_err:
                                        self.logger.warning(f"Error processing StructScalar for tags (batch row ~{current_row_in_batch_tags_processing + i}): {struct_scalar_err}. Defaulting to empty string.")
                                else: # General case for other scalars (StringScalar, etc.)
                                    py_val = scalar_val.as_py()
                                    if isinstance(py_val, (list, dict)):
                                        try:
                                            current_tag_str = json.dumps(py_val)
                                        except TypeError:
                                            self.logger.warning(f"Tag value (batch row ~{current_row_in_batch_tags_processing + i}) is list/dict but not JSON serializable: {type(py_val)}. Using str().")
                                            current_tag_str = str(py_val)
                                    elif py_val is not None:
                                        current_tag_str = str(py_val)
                            tags_strings.append(current_tag_str)
                        current_row_in_batch_tags_processing += len(arr_chunk)
                else:
                    # Handle unexpected types for tags_column_data (e.g. direct Python list/dict if CSV reader was very aggressive)
                    self.logger.warning(f"Unexpected type for Tags column data: {type(tags_column_data)}. Attempting best-effort conversion for {len(batch)} rows.")
                    if isinstance(tags_column_data, list): # e.g. if to_pylist() was called on the whole column by mistake somewhere
                        for item in tags_column_data:
                            if isinstance(item, str):
                                tags_strings.append(item)
                            elif isinstance(item, (list, dict)):
                                try: tags_strings.append(json.dumps(item))
                                except: tags_strings.append(str(item))
                            elif item is None: tags_strings.append("")
                            else: tags_strings.append(str(item))
                        # Ensure tags_strings length matches batch length
                        if len(tags_strings) != len(batch):
                            self.logger.error(f"Tags list length {len(tags_strings)} mismatch with batch length {len(batch)} after processing unexpected list type. Padding with empty strings.")
                            tags_strings.extend([""] * (len(batch) - len(tags_strings)))
                    elif isinstance(tags_column_data, pa.Scalar): # A scalar type not caught above, for the whole column
                        val_str = str(tags_column_data.as_py()) if tags_column_data.is_valid else ""
                        tags_strings = [val_str] * len(batch)
                    else:
                        self.logger.error(f"Cannot process Tags column of unhandled type {type(tags_column_data)}. Defaulting to empty tags for all {len(batch)} rows.")
                        tags_strings = [""] * len(batch)

                # Common processing for successfully generated tags_strings
                parsed_tags = [self.tag_parser.parse_tags_json(tags_str) for tags_str in tags_strings]
                tags_array = self.tag_parser.convert_to_arrow_array(parsed_tags)
                arrays[tags_index] = tags_array
                fields[tags_index] = pa.field("Tags", self.tag_parser.get_tags_type())
                self.logger.debug("Processed and converted Tags column to structured format")
            
            except Exception as e:
                self.logger.error(f"Error processing Tags column: {e}", exc_info=True)
                # If an error occurs, we aim to keep the original Tags column structure if possible,
                # or at least not crash. The 'fields' list would still contain the original field type for Tags.
                # The original 'arrays[tags_index]' is preserved by not reassigning it here.
        
        # Create a new schema with the updated fields
        schema = pa.schema(fields)
        
        # Create a new batch with the updated arrays and schema
        return pa.RecordBatch.from_arrays(arrays, schema=schema)
