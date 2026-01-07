"""
Logging Utilities for EMS ETL Pipeline
Author: J. Ryan Butcher

Provides structured logging to both database and file/console.
"""

import sqlite3
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path
from contextlib import contextmanager

from config import get_config


@dataclass
class StepMetrics:
    """Metrics for a single ETL step."""
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_rejected: int = 0
    rows_deleted: int = 0


@dataclass
class ErrorRecord:
    """Single error record for logging."""
    source_row_num: int
    error_type: str
    error_message: str
    column_name: Optional[str] = None
    column_value: Optional[str] = None
    source_data: Optional[str] = None


class ETLLogger:
    """
    ETL logging handler for database and file/console logging.

    Tracks run-level and step-level metrics, captures errors,
    and provides structured logging output.
    """

    def __init__(self, db_path: str = None):
        """
        Initialize ETL logger.

        Args:
            db_path: Path to SQLite database (uses config default if None)
        """
        config = get_config()
        self.db_path = db_path or config.database.sqlite_path

        # Setup Python logging
        self._setup_file_logger(config)

        # Run tracking
        self.run_id: Optional[int] = None
        self.step_order: int = 0
        self.errors: List[ErrorRecord] = []

        # Ensure logging tables exist
        self._init_logging_tables()

    def _setup_file_logger(self, config):
        """Configure Python file/console logger."""
        self.logger = logging.getLogger("ems_etl")
        self.logger.setLevel(getattr(logging, config.logging.level))

        # Clear existing handlers
        self.logger.handlers = []

        # File handler
        log_path = Path(config.logging.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
        )
        self.logger.addHandler(file_handler)

        # Console handler
        if config.logging.console_output:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
            )
            self.logger.addHandler(console_handler)

    def _init_logging_tables(self):
        """Create logging tables if they don't exist."""
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS ETL_RUN_LOG (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_start_dt TEXT NOT NULL,
                    run_end_dt TEXT,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    source_file TEXT,
                    source_row_count INTEGER,
                    environment TEXT,
                    load_type TEXT,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS ETL_STEP_LOG (
                    step_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    step_name TEXT NOT NULL,
                    step_order INTEGER,
                    step_start_dt TEXT NOT NULL,
                    step_end_dt TEXT,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    rows_read INTEGER DEFAULT 0,
                    rows_inserted INTEGER DEFAULT 0,
                    rows_updated INTEGER DEFAULT 0,
                    rows_rejected INTEGER DEFAULT 0,
                    error_message TEXT,
                    FOREIGN KEY (run_id) REFERENCES ETL_RUN_LOG(run_id)
                );

                CREATE TABLE IF NOT EXISTS ETL_ERROR_LOG (
                    error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    step_name TEXT,
                    error_dt TEXT NOT NULL,
                    source_row_num INTEGER,
                    error_type TEXT,
                    error_message TEXT,
                    column_name TEXT,
                    column_value TEXT,
                    source_data TEXT,
                    FOREIGN KEY (run_id) REFERENCES ETL_RUN_LOG(run_id)
                );

                CREATE INDEX IF NOT EXISTS IX_ETL_STEP_RUN ON ETL_STEP_LOG(run_id);
                CREATE INDEX IF NOT EXISTS IX_ETL_ERROR_RUN ON ETL_ERROR_LOG(run_id);
            """)

    @contextmanager
    def _get_connection(self):
        """Get database connection context manager."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def start_run(
        self,
        source_file: str,
        environment: str = "dev",
        load_type: str = "full"
    ) -> int:
        """
        Start a new ETL run and return run_id.

        Args:
            source_file: Name of source file being processed
            environment: Environment name
            load_type: Type of load (full/incremental)

        Returns:
            run_id for this execution
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ETL_RUN_LOG (run_start_dt, source_file, environment, load_type)
                VALUES (?, ?, ?, ?)
                """,
                (datetime.now().isoformat(), source_file, environment, load_type)
            )
            self.run_id = cursor.lastrowid

        self.step_order = 0
        self.errors = []
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"ETL Run Started - Run ID: {self.run_id}")
        self.logger.info(f"Source: {source_file} | Environment: {environment}")
        self.logger.info(f"{'=' * 60}")

        return self.run_id

    def end_run(
        self,
        status: str = "SUCCESS",
        source_row_count: int = None,
        error_message: str = None
    ):
        """
        End the current ETL run.

        Args:
            status: Final status (SUCCESS, FAILED, PARTIAL)
            source_row_count: Total rows in source file
            error_message: Error message if failed
        """
        if self.run_id is None:
            return

        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE ETL_RUN_LOG
                SET run_end_dt = ?, status = ?, source_row_count = ?, error_message = ?
                WHERE run_id = ?
                """,
                (datetime.now().isoformat(), status, source_row_count, error_message, self.run_id)
            )

        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"ETL Run Completed - Status: {status}")
        if source_row_count:
            self.logger.info(f"Total source rows: {source_row_count:,}")
        if error_message:
            self.logger.error(f"Error: {error_message}")
        self.logger.info(f"{'=' * 60}")

    @contextmanager
    def step(self, step_name: str):
        """
        Context manager for ETL step logging.

        Args:
            step_name: Name of the step

        Yields:
            StepMetrics instance to update during step
        """
        self.step_order += 1
        step_start = datetime.now()
        metrics = StepMetrics()

        self.logger.info(f"[Step {self.step_order}] {step_name} - Starting...")

        # Insert step start record
        step_log_id = None
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ETL_STEP_LOG (run_id, step_name, step_order, step_start_dt)
                VALUES (?, ?, ?, ?)
                """,
                (self.run_id, step_name, self.step_order, step_start.isoformat())
            )
            step_log_id = cursor.lastrowid

        try:
            yield metrics
            status = "SUCCESS"
            error_msg = None
        except Exception as e:
            status = "FAILED"
            error_msg = str(e)
            self.logger.error(f"[Step {self.step_order}] {step_name} - FAILED: {e}")
            raise
        finally:
            # Update step end record
            step_end = datetime.now()
            duration = (step_end - step_start).total_seconds()

            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE ETL_STEP_LOG
                    SET step_end_dt = ?, status = ?,
                        rows_read = ?, rows_inserted = ?, rows_updated = ?,
                        rows_rejected = ?, error_message = ?
                    WHERE step_log_id = ?
                    """,
                    (
                        step_end.isoformat(), status,
                        metrics.rows_read, metrics.rows_inserted, metrics.rows_updated,
                        metrics.rows_rejected, error_msg, step_log_id
                    )
                )

            self.logger.info(
                f"[Step {self.step_order}] {step_name} - {status} "
                f"({duration:.2f}s) | "
                f"Read: {metrics.rows_read:,} | "
                f"Inserted: {metrics.rows_inserted:,} | "
                f"Rejected: {metrics.rows_rejected:,}"
            )

    def log_error(
        self,
        step_name: str,
        error: ErrorRecord
    ):
        """
        Log a single error record.

        Args:
            step_name: Name of the step where error occurred
            error: ErrorRecord with error details
        """
        self.errors.append(error)

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO ETL_ERROR_LOG
                (run_id, step_name, error_dt, source_row_num, error_type,
                 error_message, column_name, column_value, source_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.run_id, step_name, datetime.now().isoformat(),
                    error.source_row_num, error.error_type, error.error_message,
                    error.column_name, error.column_value, error.source_data
                )
            )

    def info(self, message: str):
        """Log info message."""
        self.logger.info(message)

    def warning(self, message: str):
        """Log warning message."""
        self.logger.warning(message)

    def error(self, message: str):
        """Log error message."""
        self.logger.error(message)

    def debug(self, message: str):
        """Log debug message."""
        self.logger.debug(message)


# Global logger instance
_logger: Optional[ETLLogger] = None


def get_logger(db_path: str = None) -> ETLLogger:
    """Get or create global ETL logger instance."""
    global _logger
    if _logger is None:
        _logger = ETLLogger(db_path)
    return _logger


if __name__ == "__main__":
    # Test logging
    logger = get_logger("./test_log.db")
    run_id = logger.start_run("test_file.csv", "dev", "full")

    with logger.step("Test Step") as metrics:
        metrics.rows_read = 100
        metrics.rows_inserted = 95
        metrics.rows_rejected = 5

    logger.end_run("SUCCESS", 100)
