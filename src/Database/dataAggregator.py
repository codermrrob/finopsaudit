import pandas as pd
import re
import unicodedata
import logging

logger = logging.getLogger(__name__)
from typing import Optional, List

from .duckdb_manager import DuckDBManager
from Configuration import RegularExpressions, ResourcesPerDayJsonColumns, ResourcesPerDayJsonColumnSets


class DataAggregator:
    """Encapsulates data loading and aggregation logic using DuckDBManager."""
    
    def __init__(self, db_manager: DuckDBManager):
        """
        Initialize with a DuckDBManager instance.
        
        Args:
            db_manager: DuckDBManager instance for database operations
        """
        self.db_manager = db_manager

    def extract_resource_group(self, resource_id: str) -> Optional[str]:
        """Extract resource group from resource ID using regex pattern."""
        if not isinstance(resource_id, str):
            return None
        
        # You'll need to define this pattern in your config
        pattern = re.compile(RegularExpressions.RESOURCE_GROUP_REGEX_PATTERN, re.IGNORECASE)
        match = pattern.search(resource_id)
        return match.group(1) if match else None

    def load_monthly(
        self, 
        view_name: str, 
        year: int, 
        month: str, 
        normalize_text: bool = False
    ) -> pd.DataFrame:
        """
        Load and aggregate monthly data from a view.
        
        Args:
            view_name: Name of the database view to query
            year: Year to filter by
            month: Month to filter by
            normalize_text: Whether to apply NFKC normalization and lowercasing
            
        Returns:
            Aggregated DataFrame with monthly data
        """
        query = self._build_monthly_query(view_name, year, month)
        
        try:
            logger.info(f"Loading monthly data for {year}-{month} from '{view_name}'")
            
            with self.db_manager.connection(read_only=True) as conn:
                df = conn.execute(query).fetchdf()
            
            if df.empty:
                error_msg = f"No data found for {year}-{month} in view '{view_name}'"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Apply transformations
            df = self._apply_transformations(df, normalize_text)
            
            logger.info(f"Successfully loaded {len(df)} records for {year}-{month}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load data for {year}-{month}: {e}")
            self._log_debug_info(view_name, query)
            raise

    def _build_monthly_query(self, view_name: str, year: int, month: str) -> str:
        """Build the SQL query for monthly data aggregation."""
        return f"""
        WITH MonthlyData AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY ResourceId ORDER BY day DESC) as rn
            FROM {view_name}
            WHERE year = {year} AND month = '{month}'
        )
        SELECT
            ResourceId as ResourceId,
            any_value(ResourceName) as ResourceName,
            any_value(ResourceType) as ResourceType,
            SUM(TotalEffectiveCost) as Cost,
            any_value(BillingAccountName) as BillingAccountName,
            any_value(SubAccountName) as SubAccountName,
            any_value(CASE WHEN rn = 1 THEN Tags ELSE NULL END) as Tags
        FROM MonthlyData
        GROUP BY ResourceId
        """

    def _apply_transformations(self, df: pd.DataFrame, normalize_text: bool) -> pd.DataFrame:
        """Apply transformations to the DataFrame."""
        # Extract resource group
        df[ResourcesPerDayJsonColumns.RESOURCE_GROUP] = df[ResourcesPerDayJsonColumns.RESOURCE_ID].apply(self.extract_resource_group)
        
        if normalize_text:
            df = self._normalize_text_columns(df)
        
        return df

    def _normalize_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply NFKC normalization and lowercasing to text columns."""
        text_columns = [
            ResourcesPerDayJsonColumns.RESOURCE_ID,
            ResourcesPerDayJsonColumns.RESOURCE_NAME,
            ResourcesPerDayJsonColumns.BILLING_ACCOUNT_NAME,
            ResourcesPerDayJsonColumns.SUB_ACCOUNT_NAME,
            ResourcesPerDayJsonColumns.TAGS,
            ResourcesPerDayJsonColumns.RESOURCE_GROUP
        ]
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: unicodedata.normalize('NFKC', x).lower() 
                    if isinstance(x, str) else x
                )
        
        return df

    def _log_debug_info(self, view_name: str, query: str) -> None:
        """Log debug information when queries fail."""
        logger.error(f"Query attempted:\n{query}")
        
        try:
            schema_info = self.db_manager.describe_view(view_name)
            logger.info(f"Schema of '{view_name}': {schema_info}")
        except Exception as desc_e:
            logger.warning(f"Could not describe view '{view_name}': {desc_e}")

    # Convenience methods for backward compatibility
    def load_monthly_NFKC(self, view_name: str, year: int, month: str) -> pd.DataFrame:
        """Load monthly data with NFKC normalization (convenience method)."""
        return self.load_monthly(view_name, year, month, normalize_text=True)