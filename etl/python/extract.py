"""
Data Extraction Module for EMS ETL Pipeline
Author: J. Ryan Butcher

Handles reading source CSV files with chunked processing for memory efficiency.
"""

import os
import csv
from pathlib import Path
from typing import Iterator, Dict, Any, List, Tuple
from dataclasses import dataclass

from config import get_config


@dataclass
class ExtractionResult:
    """Result of extraction operation."""
    file_path: str
    total_rows: int
    columns: List[str]
    success: bool
    error_message: str = None


def count_csv_rows(file_path: str) -> int:
    """
    Count total rows in CSV file (excluding header).

    Args:
        file_path: Path to CSV file

    Returns:
        Number of data rows
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1  # Subtract header


def get_csv_columns(file_path: str) -> List[str]:
    """
    Get column names from CSV header.

    Args:
        file_path: Path to CSV file

    Returns:
        List of column names
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        return next(reader)


def extract_csv_chunks(
    file_path: str,
    chunk_size: int = None
) -> Iterator[Tuple[List[Dict[str, Any]], int]]:
    """
    Extract CSV data in memory-efficient chunks.

    Args:
        file_path: Path to source CSV file
        chunk_size: Number of rows per chunk (uses config default if None)

    Yields:
        Tuple of (list of row dicts, starting row number)
    """
    config = get_config()
    chunk_size = chunk_size or config.etl.batch_size

    with open(file_path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        chunk = []
        row_num = 1  # Start after header

        for row in reader:
            # Add row number for tracing
            row['_source_row_num'] = row_num
            chunk.append(row)
            row_num += 1

            if len(chunk) >= chunk_size:
                yield chunk, row_num - len(chunk)
                chunk = []

        # Yield remaining rows
        if chunk:
            yield chunk, row_num - len(chunk)


def find_source_files(pattern: str = "*.csv") -> List[str]:
    """
    Find source files matching pattern in configured source directory.

    Args:
        pattern: Glob pattern for files

    Returns:
        List of absolute file paths
    """
    config = get_config()
    source_dir = Path(config.etl.source_path)

    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    files = list(source_dir.glob(pattern))
    return [str(f.absolute()) for f in files]


def validate_source_file(file_path: str) -> ExtractionResult:
    """
    Validate source file exists and is readable.

    Args:
        file_path: Path to source file

    Returns:
        ExtractionResult with validation status
    """
    path = Path(file_path)

    if not path.exists():
        return ExtractionResult(
            file_path=file_path,
            total_rows=0,
            columns=[],
            success=False,
            error_message=f"File not found: {file_path}"
        )

    if not path.is_file():
        return ExtractionResult(
            file_path=file_path,
            total_rows=0,
            columns=[],
            success=False,
            error_message=f"Not a file: {file_path}"
        )

    try:
        columns = get_csv_columns(file_path)
        total_rows = count_csv_rows(file_path)

        return ExtractionResult(
            file_path=file_path,
            total_rows=total_rows,
            columns=columns,
            success=True
        )

    except Exception as e:
        return ExtractionResult(
            file_path=file_path,
            total_rows=0,
            columns=[],
            success=False,
            error_message=str(e)
        )


def archive_source_file(file_path: str) -> str:
    """
    Move source file to archive directory with timestamp.

    Args:
        file_path: Path to source file

    Returns:
        New path in archive directory
    """
    from datetime import datetime

    config = get_config()
    archive_dir = Path(config.etl.archive_path)
    archive_dir.mkdir(parents=True, exist_ok=True)

    source = Path(file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = f"{source.stem}_{timestamp}{source.suffix}"
    archive_path = archive_dir / archive_name

    source.rename(archive_path)
    return str(archive_path)


if __name__ == "__main__":
    # Test extraction
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "./data/input/ems_runs_2024.csv"

    result = validate_source_file(file_path)
    print(f"File: {result.file_path}")
    print(f"Valid: {result.success}")
    print(f"Rows: {result.total_rows:,}")
    print(f"Columns: {len(result.columns)}")

    if result.success:
        print("\nFirst chunk preview:")
        for chunk, start_row in extract_csv_chunks(file_path, chunk_size=5):
            for row in chunk[:2]:
                print(f"  Row {row['_source_row_num']}: {row.get('INCIDENT_DT')} - {row.get('INCIDENT_COUNTY')}")
            break
