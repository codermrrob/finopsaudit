"""
DuckDB database manager for FOCUS data operations.
"""

import logging
import json
from pathlib import Path
from contextlib import contextmanager
from typing import Generator, Dict, Any

import duckdb

from FileSystem.base import FileSystem


class DuckDBManager:
    """
    Manages DuckDB database operations for FOCUS data.
    
    Handles database creation, view management, and provides connection management
    for tenant-specific DuckDB databases.
    """

    def __init__(self, output_base: str, tenant: str):
        """
        Initialize the DuckDB manager.

        Args:
            fs: The filesystem to use for operations
            output_base: The base data path
            tenant: The tenant ID
        """
        self.output_base = output_base
        self.tenant = tenant
        self.logger = logging.getLogger(__name__)

        # Database path construction
        self.db_path = self._get_database_path()
        self.views_dir = Path(__file__).parent / "duckdb_views"

    def _get_database_path(self) -> Path:
        """Get the path for the tenant's database file."""
        duckdb_dir = Path(self.output_base) / self.tenant / 'duckdb'
        duckdb_dir.mkdir(parents=True, exist_ok=True)
        return duckdb_dir / f"{self.tenant}.db"

    @contextmanager
    def connection(self, read_only: bool = False) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Context manager for DuckDB connections.
        
        Args:
            read_only: Whether to open in read-only mode
            
        Yields:
            DuckDB connection
        """
        conn = None
        try:
            conn = duckdb.connect(database=str(self.db_path), read_only=read_only)
            self.logger.debug(f"Connected to DuckDB at {self.db_path}")
            yield conn
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                self.logger.debug("Database connection closed")

    def update_views(self) -> None:
        """Create or replace views in the DuckDB database."""
        self.logger.info(f"Updating DuckDB views for tenant {self.tenant}")

        try:
            with self.connection(read_only=False) as conn:
                self._create_base_views(conn)
                self._create_dependent_views(conn)
            
            self.logger.info(f"Successfully updated DuckDB database at {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to update DuckDB views: {e}", exc_info=True)
            raise

    def describe_view(self, view_name: str) -> Dict[str, Any]:
        """
        Describe a view's schema.
        
        Args:
            view_name: Name of the view to describe
            
        Returns:
            Dictionary containing schema information
        """
        with self.connection(read_only=True) as conn:
            try:
                result = conn.execute(f"DESCRIBE {view_name};").fetchall()
                columns = [desc[0] for desc in conn.description]
                
                schema_info = []
                for row in result:
                    schema_info.append(dict(zip(columns, row)))
                
                self.logger.info(f"Retrieved schema for view '{view_name}'")
                return {"view_name": view_name, "schema": schema_info}
            except Exception as e:
                self.logger.error(f"Could not describe view '{view_name}': {e}")
                raise

    def _create_base_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create base views that read directly from Parquet datasets."""
        self.logger.info("Creating base views")
        
        parquet_base_path = Path(self.output_base) / self.tenant / 'parquet'
        costs_path = (parquet_base_path / 'costs_raw').as_posix()

        view_sql = f"""
            CREATE OR REPLACE VIEW all_costs_view AS 
            SELECT * FROM read_parquet('{costs_path}/year=*/month=*/day=*/*.parquet', 
                                     HIVE_PARTITIONING = 1)
        """
        
        self.logger.debug(f"Creating all_costs_view with path: {costs_path}")
        conn.execute(view_sql)
        self.logger.info("Base view 'all_costs_view' created successfully")

    def _create_dependent_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create dependent views from SQL files."""
        self.logger.info("Creating dependent views from SQL files")

        # Use FileSystem to read the config file (if it should be part of filesystem)
        # Or keep as local file if it's part of the application code
        order_config_path = self.views_dir / 'view_order.json'
        
        if not order_config_path.exists():
            self.logger.error(f"View order config not found: {order_config_path}")
            raise FileNotFoundError(f"View order config file not found: {order_config_path}")

        with open(order_config_path, 'r') as f:
            view_config = json.load(f)
        
        execution_order = view_config.get("view_execution_order", [])
        
        if not execution_order:
            self.logger.warning("No views specified in execution order")
            return

        for view_file_name in execution_order:
            self._create_view_from_file(conn, view_file_name)

    def _create_view_from_file(self, conn: duckdb.DuckDBPyConnection, view_file_name: str) -> None:
        """Create a single view from SQL file."""
        sql_file = self.views_dir / view_file_name
        
        if not sql_file.exists():
            self.logger.warning(f"View file not found: {view_file_name}")
            return

        self.logger.info(f"Creating view from: {view_file_name}")
        
        try:
            with open(sql_file, "r") as f:
                sql_query = f.read()
            
            conn.execute(sql_query)
            self.logger.debug(f"Successfully created view from {view_file_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to create view from {view_file_name}: {e}")
            raise