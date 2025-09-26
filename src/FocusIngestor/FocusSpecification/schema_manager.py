"""
Manages the FOCUS specification schema by parsing a YAML definition file.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple
import pyarrow as pa

class FocusSchemaManager:
    """
    Parses the FOCUS column specification YAML and provides the schema
    and column requirement details.
    """

    def __init__(self):
        self.spec_path = Path(__file__).parent / "focus_col_spec.yaml"
        self.logger = logging.getLogger(__name__)
        self.spec = self._load_and_parse_spec()
        self.schema = self._build_pyarrow_schema()
        self.mandatory_columns = self._get_columns_by_feature_level(["Mandatory"])
        self.feature_level_map = {col.get('id'): col.get('feature level', 'N/A') for col in self.spec}

    def _load_and_parse_spec(self) -> List[Dict[str, Any]]:
        """
        Loads the custom-formatted YAML spec and parses it into a list of dicts.
        """
        self.logger.info(f"Loading FOCUS specification from: {self.spec_path}")
        if not self.spec_path.exists():
            self.logger.error(f"Specification file not found at {self.spec_path}")
            raise FileNotFoundError(f"Specification file not found: {self.spec_path}")

        spec_list = []
        current_column = None

        with open(self.spec_path, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if not stripped_line:
                    continue

                if stripped_line.startswith('column'):
                    # Finalize the previous column if it exists
                    if current_column:
                        spec_list.append(current_column)
                    
                    # Start a new column
                    col_name = stripped_line.replace('column "', '"')[:-1]
                    current_column = {'column_name_from_spec': col_name}

                elif stripped_line.startswith('- ') and current_column is not None:
                    # This is a property of the current column
                    try:
                        # Correctly parse the key-value pair
                        key_part, value_part = stripped_line[2:].split(':', 1)
                        key = key_part.strip()
                        value = value_part.strip()
                        current_column[key] = value
                    except ValueError:
                        self.logger.warning(f"Could not parse property line: '{stripped_line}'. Skipping.")
        
        # Append the last column after the loop finishes
        if current_column:
            spec_list.append(current_column)

        self.logger.info(f"Successfully parsed {len(spec_list)} column definitions.")
        return spec_list

    def _build_pyarrow_schema(self) -> pa.Schema:
        """
        Builds a PyArrow schema from the parsed specification.
        """
        type_mapping = {
            "String": pa.string(),
            "Date/Time": pa.timestamp('s', tz='UTC'),
            "Decimal": pa.decimal128(38, 18),  # Default precision for metrics
            "JSON": pa.string()
        }

        fields = []
        for col_spec in self.spec:
            col_id = col_spec.get('id')
            col_type_str = col_spec.get('data type')

            if not col_id or not col_type_str:
                self.logger.warning(f"Skipping column due to missing 'id' or 'data type' in spec: {col_spec}")
                continue

            pa_type = type_mapping.get(col_type_str)
            if not pa_type:
                self.logger.warning(f"Unknown data type '{col_type_str}' for column '{col_id}'. Defaulting to string.")
                pa_type = pa.string()
            
            # Ensure all decimal types use high precision to match original working behavior
            if pa.types.is_decimal(pa_type):
                pa_type = pa.decimal128(38, 32)

            fields.append(pa.field(col_id, pa_type))

        # Add partition columns that are not in the spec, matching DuckDB schema
        fields.extend([
            pa.field("year", pa.int64()),      # BIGINT
            pa.field("month", pa.string()),    # VARCHAR
            pa.field("day", pa.string()),      # VARCHAR
        ])

        return pa.schema(fields)

    def _get_columns_by_feature_level(self, levels: List[str]) -> List[str]:
        """
        Gets a list of column IDs for the given feature levels.
        """
        return [
            col.get('id') for col in self.spec 
            if col.get('feature level') in levels and col.get('id')
        ]
