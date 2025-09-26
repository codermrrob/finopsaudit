# FOCUS Ingest

A Python service for ingesting FOCUS-format CSV exports into date-partitioned Parquet datasets.

## Overview

This package provides a streaming ETL pipeline that:

1. Reads FOCUS-formatted CSV files with cost and tag data
2. Converts them to Parquet format with date partitioning (`year/month/day`)
3. Maintains raw tag data and creates a separate tag index
4. Ensures high-precision decimal handling for cost fields (DECIMAL128(38,32))
5. Supports local and cloud storage (AWS S3, Azure Blob Storage)

## Installation

```bash
# From PyPI (when published)
pip install focus-ingest

# From source
pip install -e .

# With cloud storage support
pip install -e .[cloud]

# With development dependencies
pip install -e .[dev]
```

## Usage

### Command Line

```bash
# Basic usage
focus-ingest --input "file:///data/input/**/*.csv" --output "file:///data/output"

# With options
focus-ingest \
  --input "file:///data/input/**/*.csv" \
  --output "file:///data/output" \
  --batch-size 200000 \
  --compression snappy \
  --overwrite \
  --fs local \
  --log-dir "./logs"
```

### Python API

```python
from focus_ingest.filesystem.registry import get_filesystem
from focus_ingest.service import IngestionService

# Create the filesystem
fs = get_filesystem("local")

# Create the ingestion service
service = IngestionService(
    fs=fs,
    output_base="file:///data/output",
    batch_size=200000,
    compression="snappy",
    overwrite=True
)

# Run the ingestion
service.ingest("file:///data/input/**/*.csv")
```

## Architecture

The service follows a repository pattern with these key components:

1. **FileSystem Abstraction**: Pluggable filesystem implementations
2. **CSV Parsing**: Streaming reader with decimal precision handling
3. **Parquet Writing**: Date-partitioned writers for `costs_raw` and `tags_index`
4. **Tag Handling**: JSON parsing and tag explosion
5. **Service Layer**: Orchestration and configuration

## Output Format

The service produces two Parquet datasets:

### costs_raw

Contains the original CSV data with:
- All cost fields cast to DECIMAL128(38,32)
- Tags parsed into a structured array
- Date partitioning columns added (`year`, `month`, `day`)

### tags_index

Contains a flattened view of resource tags:
- One row per `(resource_id, tag_key, tag_value, date)`
- Date partitioning columns (`year`, `month`, `day`)

Both datasets are partitioned by `year/month/day` based on the `ChargePeriodStart` field.

## Development

```bash
# Install development dependencies
pip install -e .[dev]

# Run tests
pytest

# Run linting
black .
isort .
mypy .
```
