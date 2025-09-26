"""
Parquet writer manager for FOCUS ingestion service.

This module provides a manager for Parquet writers that handles partitioning
and batch writes using PyArrow directly for high-performance streaming writes.
"""

import logging
from typing import Dict, Tuple, List

import pyarrow as pa
import pyarrow.parquet as pq

from FileSystem.base import FileSystem
from Utils.partitionutils import get_partition_path


class ParquetWriterManager:
    """
    Manager for PyArrow Parquet writers.
    
    This class manages multiple PyArrow Parquet writers for different date partitions
    and datasets, ensuring that data is written to the correct partition with optimal
    performance for streaming writes.
    
    Note: This uses FileSystem directly (not DataManager) because it requires
    low-level stream control for PyArrow ParquetWriter operations.
    """
    
    def __init__(
        self,
        fs: FileSystem,
        output_base: str,
        tenant: str,
        compression: str = "snappy",
        overwrite: bool = True
    ) -> None:
        """
        Initialize the Parquet writer manager.
        
        Args:
            fs: The filesystem to use for writing files
            output_base: The base data path
            tenant: The tenant identifier
            compression: The compression codec to use (default: "snappy")
            overwrite: Whether to overwrite existing partitions (default: True)
        """
        self.fs = fs
        self.output_base = output_base
        self.tenant = tenant
        self.compression = compression
        self.overwrite = overwrite
        self.logger = logging.getLogger(__name__)
        
        # Dictionary of (dataset, year, month, day) -> writer
        self.writers: Dict[Tuple[str, int, int, int], pq.ParquetWriter] = {}
        
        # Dictionary of (dataset, year, month, day) -> schema
        self.schemas: Dict[Tuple[str, int, int, int], pa.Schema] = {}
    
    def get_writer(
        self, dataset: str, year: int, month: int, day: int, schema: pa.Schema
    ) -> pq.ParquetWriter:
        """
        Get or create a PyArrow ParquetWriter for the specified dataset and partition.
        
        Args:
            dataset: The name of the dataset (e.g., "costs_raw" or "tags_index")
            year: The year
            month: The month
            day: The day
            schema: The PyArrow schema for the data
            
        Returns:
            A PyArrow ParquetWriter instance
            
        Raises:
            IOError: If the writer cannot be created
        """
        key = (dataset, year, month, day)
        
        # Return existing writer if it exists
        if key in self.writers:
            existing_schema = self.schemas[key]
            if not existing_schema.equals(schema):
                self.logger.warning(
                    f"Schema mismatch for partition {key}! "
                    f"Existing: {existing_schema.names}, Current: {schema.names}. "
                    f"This may cause write errors."
                )
                self.logger.debug(f"Existing schema: {existing_schema}")
                self.logger.debug(f"Current schema: {schema}")
            return self.writers[key]
        
        # Create new writer
        writer = self._create_writer(dataset, year, month, day, schema)
        self.writers[key] = writer
        self.schemas[key] = schema
        
        return writer
    
    def _create_writer(
        self, dataset: str, year: int, month: int, day: int, schema: pa.Schema
    ) -> pq.ParquetWriter:
        """
        Create a new PyArrow ParquetWriter for the specified partition.
        
        Args:
            dataset: The name of the dataset
            year: The year
            month: The month  
            day: The day
            schema: The PyArrow schema
            
        Returns:
            A PyArrow ParquetWriter instance
        """
        # Generate partition and file paths
        partition_path = get_partition_path(self.output_base, self.tenant, dataset, year, month, day)
        file_path = f"{partition_path}/data_{year}{month:02d}{day:02d}.parquet"
        
        self.logger.info(f"Creating writer for partition: {partition_path}")
        self.logger.debug(f"Parquet file path: {file_path}")

        # Handle existing partition
        if self.overwrite and self.fs.exists(partition_path):
            self.logger.info(f"Removing existing partition: {partition_path}")
            self.fs.remove(partition_path, recursive=True)
        
        # Ensure partition directory exists
        self.fs.mkdirs(partition_path)
        
        # Create writer with output stream
        output_stream = self.fs.open_output_stream(file_path)
        
        return pq.ParquetWriter(
            output_stream,
            schema,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True
        )
    
    def write_batch(
        self,
        dataset: str,
        batch: pa.RecordBatch,
        year: int,
        month: int,
        day: int
    ) -> None:
        """
        Write a PyArrow RecordBatch to the specified dataset and partition.
        
        Args:
            dataset: The name of the dataset
            batch: The PyArrow RecordBatch to write
            year: The year
            month: The month
            day: The day
            
        Raises:
            IOError: If the batch cannot be written
        """
        writer = self.get_writer(dataset, year, month, day, batch.schema)
        
        self.logger.debug(
            f"Writing {len(batch)} rows to {dataset}/year={year}/month={month:02d}/day={day:02d}"
        )
        
        writer.write_batch(batch)
    
    def write_batches_by_partition(
        self,
        dataset: str,
        batch: pa.RecordBatch,
        partition_info: List[Tuple[int, int, int]]
    ) -> Dict[Tuple[int, int, int], int]:
        """
        Write a batch to multiple partitions based on partition_info.
        
        This method splits a batch into multiple partitions based on the
        partition_info list, which contains (year, month, day) tuples for
        each row in the batch.
        
        Args:
            dataset: The name of the dataset
            batch: The PyArrow RecordBatch to write
            partition_info: A list of (year, month, day) tuples, one for each row
            
        Returns:
            A dictionary of (year, month, day) -> row count
            
        Raises:
            IOError: If a batch cannot be written
        """
        self.logger.debug(f"Writing batch to multiple partitions for dataset '{dataset}'")
        self.logger.debug(f"Batch schema: {batch.schema.names}")
        self.logger.debug(f"Partition info sample: {partition_info[:5]}")
        
        # Group rows by partition
        partition_indices: Dict[Tuple[int, int, int], List[int]] = {}
        
        for i, (year, month, day) in enumerate(partition_info):
            key = (year, month, day)
            if key not in partition_indices:
                partition_indices[key] = []
            partition_indices[key].append(i)
        
        # Write each partition
        partition_counts: Dict[Tuple[int, int, int], int] = {}
        
        for partition_key, indices in partition_indices.items():
            year, month, day = partition_key
            
            self.logger.debug(f"Processing partition {partition_key} with {len(indices)} rows")
            
            # Extract rows for this partition
            partition_batch = batch.take(pa.array(indices))
            
            # Write the partition batch
            self.write_batch(dataset, partition_batch, year, month, day)
            
            # Store the row count
            partition_counts[partition_key] = len(partition_batch)
        
        return partition_counts
    
    def close(self) -> None:
        """
        Close all PyArrow ParquetWriters.
        
        This method should be called when the ingestion is complete to ensure
        that all data is flushed to disk and files are properly closed.
        """
        if not self.writers:
            self.logger.debug("No writers to close")
            return
            
        self.logger.info(f"Closing {len(self.writers)} Parquet writers")
        
        for key, writer in self.writers.items():
            dataset, year, month, day = key
            self.logger.debug(f"Closing writer for {dataset}/year={year}/month={month:02d}/day={day:02d}")
            try:
                writer.close()
            except Exception as e:
                self.logger.error(f"Error closing writer for {key}: {e}")
        
        # Clear the dictionaries
        self.writers.clear()
        self.schemas.clear()
        
        self.logger.info("All Parquet writers closed successfully")