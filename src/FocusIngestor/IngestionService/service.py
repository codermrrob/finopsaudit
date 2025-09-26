"""
Main ingestion service for FOCUS CSV to Parquet conversion.

This module provides the main ingestion service that orchestrates the process
of reading FOCUS CSV files and writing them to Parquet datasets.
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Tuple

import pyarrow as pa

from FileSystem.base import FileSystem
from ..Parser.csv_reader import CSVReader
from ..Parser.decimal_handler import DecimalHandler
from ..Writer.parquet_manager import ParquetWriterManager
from ..Writer.duckdb_manager import DuckDBManager


class IngestionService:
    """
    Main ingestion service for FOCUS CSV to Parquet conversion.

    This class orchestrates the process of reading FOCUS CSV files and writing
    them to Parquet datasets.
    """

    def __init__(
        self,
        fs: FileSystem,
        output_base: str,
        tenant: str,
        batch_size: int = 100000,
        compression: str = "snappy",
        overwrite: bool = True
    ) -> None:
        """
        Initialize the ingestion service.

        Args:
            fs: The filesystem to use for reading and writing files
            output_base: The base path for output files
            batch_size: The number of rows to process in each batch
            compression: The compression codec to use for Parquet files
            overwrite: Whether to overwrite existing partitions
        """
        self.fs = fs
        self.output_base = output_base
        self.tenant = tenant
        self.batch_size = batch_size
        self.compression = compression
        self.overwrite = overwrite
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.decimal_handler = DecimalHandler()
        self.costs_writer_manager = ParquetWriterManager(
            fs=self.fs,
            tenant=self.tenant,
            output_base=self.output_base
        )
        self.duckdb_manager = DuckDBManager(
            fs=self.fs,
            output_base=self.output_base,
            tenant=self.tenant
        )

    def ingest(self, input_pattern: str) -> Dict[str, Any]:
        """
        Ingest CSV files matching the pattern.
        """
        self.logger.info(f"Starting ingestion from {input_pattern} to {self.output_base}")

        try:
            file_uris = self.fs.list_files(input_pattern)
            if not file_uris:
                self.logger.warning(f"No files found matching pattern: {input_pattern}")
                return {"files_processed": 0, "rows_processed": 0, "partitions_written": 0}

            self.logger.info(f"Found {len(file_uris)} files to process")

            total_rows_processed = 0
            partitions_written = set()

            for uri in file_uris:
                try:
                    file_stats = self._process_file(uri)
                    total_rows_processed += file_stats["rows_processed"]
                    for partition in file_stats["partitions_written"]:
                        partitions_written.add(partition)
                except Exception as e:
                    self.logger.error(f"Error processing file {uri}: {e}", exc_info=True)

            self.costs_writer_manager.close()

            self._update_duckdb_views()

            stats = {
                "files_processed": len(file_uris),
                "rows_processed": total_rows_processed,
                "partitions_written": len(partitions_written)
            }
            self.logger.info(f"Ingestion completed successfully: {stats}")
            return stats

        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}")
            raise

    def _process_file(self, uri: str) -> Dict[str, Any]:
        """
        Process a single CSV file.
        """
        self.logger.info(f"Processing file: {uri}")
        rows_processed = 0
        partitions_written = set()

        try:
            for batch, metadata in CSVReader(fs=self.fs, decimal_handler=self.decimal_handler, batch_size=self.batch_size).read_batches(uri):
                rows_processed += len(batch)
                self.logger.debug(f"Processing batch with {len(batch)} rows")

                partition_counts = self._write_costs_raw(batch, metadata["partition_info"])
                for partition, count in partition_counts.items():
                    partitions_written.add(("costs_raw",) + partition)

            self.logger.info(
                f"Processed {rows_processed} rows from {uri}, "
                f"wrote to {len(partitions_written)} partitions"
            )
            return {"rows_processed": rows_processed, "partitions_written": partitions_written}

        except Exception as e:
            self.logger.error(f"Error processing file {uri}: {e}")
            raise

    def _write_costs_raw(self, batch: pa.RecordBatch, partition_info: List[Tuple[int, int, int]]) -> Dict[Tuple[int, int, int], int]:
        """
        Write a batch to the costs_raw Parquet dataset.
        """
        return self.costs_writer_manager.write_batches_by_partition(
            dataset="costs_raw",
            batch=batch,
            partition_info=partition_info
        )

    def _update_duckdb_views(self) -> None:
        """
        Update the DuckDB database with the latest views.
        """
        self.logger.info("Attempting to update DuckDB views.")
        try:
            self.duckdb_manager.update_views()
            self.logger.info("DuckDB views updated successfully.")
        except Exception as e:
            self.logger.error(f"Failed to update DuckDB views: {e}", exc_info=True)
            raise
