"""
Staging Module for EMS ETL Pipeline
Author: J. Ryan Butcher

Loads raw CSV data into staging tables, preserving original values for traceability.
"""

import sqlite3
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime

from config import get_config


# Staging table columns (matching source + audit)
STAGING_COLUMNS = [
    "_load_datetime",
    "_source_file",
    "_source_row_num",
    "INCIDENT_DT",
    "INCIDENT_COUNTY",
    "CHIEF_COMPLAINT_DISPATCH",
    "CHIEF_COMPLAINT_ANATOMIC_LOC",
    "PRIMARY_SYMPTOM",
    "PROVIDER_IMPRESSION_PRIMARY",
    "DISPOSITION_ED",
    "DISPOSITION_HOSPITAL",
    "INJURY_FLG",
    "NALOXONE_GIVEN_FLG",
    "MEDICATION_GIVEN_OTHER_FLG",
    "DESTINATION_TYPE",
    "PROVIDER_TYPE_STRUCTURE",
    "PROVIDER_TYPE_SERVICE",
    "PROVIDER_TYPE_SERVICE_LEVEL",
    "PROVIDER_TO_SCENE_MINS",
    "PROVIDER_TO_DESTINATION_MINS",
    "UNIT_NOTIFIED_BY_DISPATCH_DT",
    "UNIT_ARRIVED_ON_SCENE_DT",
    "UNIT_ARRIVED_TO_PATIENT_DT",
    "UNIT_LEFT_SCENE_DT",
    "PATIENT_ARRIVED_DESTINATION_DT"
]


def init_staging_table(db_path: str = None):
    """
    Create or recreate staging table.

    Args:
        db_path: Path to SQLite database (uses config default if None)
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        # Drop and recreate for full refresh
        conn.execute("DROP TABLE IF EXISTS STG_EMS_INCIDENT")

        conn.execute("""
            CREATE TABLE STG_EMS_INCIDENT (
                stg_load_id INTEGER PRIMARY KEY AUTOINCREMENT,
                _load_datetime TEXT,
                _source_file TEXT,
                _source_row_num INTEGER,
                INCIDENT_DT TEXT,
                INCIDENT_COUNTY TEXT,
                CHIEF_COMPLAINT_DISPATCH TEXT,
                CHIEF_COMPLAINT_ANATOMIC_LOC TEXT,
                PRIMARY_SYMPTOM TEXT,
                PROVIDER_IMPRESSION_PRIMARY TEXT,
                DISPOSITION_ED TEXT,
                DISPOSITION_HOSPITAL TEXT,
                INJURY_FLG TEXT,
                NALOXONE_GIVEN_FLG TEXT,
                MEDICATION_GIVEN_OTHER_FLG TEXT,
                DESTINATION_TYPE TEXT,
                PROVIDER_TYPE_STRUCTURE TEXT,
                PROVIDER_TYPE_SERVICE TEXT,
                PROVIDER_TYPE_SERVICE_LEVEL TEXT,
                PROVIDER_TO_SCENE_MINS TEXT,
                PROVIDER_TO_DESTINATION_MINS TEXT,
                UNIT_NOTIFIED_BY_DISPATCH_DT TEXT,
                UNIT_ARRIVED_ON_SCENE_DT TEXT,
                UNIT_ARRIVED_TO_PATIENT_DT TEXT,
                UNIT_LEFT_SCENE_DT TEXT,
                PATIENT_ARRIVED_DESTINATION_DT TEXT
            )
        """)

        conn.execute("""
            CREATE INDEX IX_STG_EMS_LOAD
            ON STG_EMS_INCIDENT (_load_datetime, _source_file)
        """)

        conn.commit()
    finally:
        conn.close()


def truncate_staging(db_path: str = None):
    """
    Truncate staging table for full refresh.

    Args:
        db_path: Path to SQLite database
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM STG_EMS_INCIDENT")
        conn.commit()
    finally:
        conn.close()


def stage_records(
    records: List[Dict[str, Any]],
    source_file: str,
    db_path: str = None
) -> int:
    """
    Insert records into staging table.

    Args:
        records: List of record dictionaries from extraction
        source_file: Name of source file for audit
        db_path: Path to SQLite database

    Returns:
        Number of records inserted
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    if not records:
        return 0

    load_datetime = datetime.now().isoformat()

    # Prepare values for bulk insert
    columns = STAGING_COLUMNS[:]  # Copy list
    placeholders = ",".join(["?" for _ in columns])
    insert_sql = f"INSERT INTO STG_EMS_INCIDENT ({','.join(columns)}) VALUES ({placeholders})"

    rows_to_insert = []
    for record in records:
        row = [
            load_datetime,
            source_file,
            record.get("_source_row_num"),
            record.get("INCIDENT_DT"),
            record.get("INCIDENT_COUNTY"),
            record.get("CHIEF_COMPLAINT_DISPATCH"),
            record.get("CHIEF_COMPLAINT_ANATOMIC_LOC"),
            record.get("PRIMARY_SYMPTOM"),
            record.get("PROVIDER_IMPRESSION_PRIMARY"),
            record.get("DISPOSITION_ED"),
            record.get("DISPOSITION_HOSPITAL"),
            record.get("INJURY_FLG"),
            record.get("NALOXONE_GIVEN_FLG"),
            record.get("MEDICATION_GIVEN_OTHER_FLG"),
            record.get("DESTINATION_TYPE"),
            record.get("PROVIDER_TYPE_STRUCTURE"),
            record.get("PROVIDER_TYPE_SERVICE"),
            record.get("PROVIDER_TYPE_SERVICE_LEVEL"),
            record.get("PROVIDER_TO_SCENE_MINS"),
            record.get("PROVIDER_TO_DESTINATION_MINS"),
            record.get("UNIT_NOTIFIED_BY_DISPATCH_DT"),
            record.get("UNIT_ARRIVED_ON_SCENE_DT"),
            record.get("UNIT_ARRIVED_TO_PATIENT_DT"),
            record.get("UNIT_LEFT_SCENE_DT"),
            record.get("PATIENT_ARRIVED_DESTINATION_DT")
        ]
        rows_to_insert.append(row)

    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(insert_sql, rows_to_insert)
        conn.commit()
        return len(rows_to_insert)
    finally:
        conn.close()


def get_staging_count(db_path: str = None) -> int:
    """
    Get count of records in staging table.

    Args:
        db_path: Path to SQLite database

    Returns:
        Number of staged records
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM STG_EMS_INCIDENT")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def get_staging_sample(db_path: str = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get sample records from staging for verification.

    Args:
        db_path: Path to SQLite database
        limit: Number of records to return

    Returns:
        List of record dictionaries
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(f"SELECT * FROM STG_EMS_INCIDENT LIMIT {limit}")
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


if __name__ == "__main__":
    # Test staging
    print("Initializing staging table...")
    init_staging_table()

    # Test with sample data
    test_records = [
        {
            "_source_row_num": 1,
            "INCIDENT_DT": "2024-01-15",
            "INCIDENT_COUNTY": "LAKE",
            "CHIEF_COMPLAINT_DISPATCH": "Chest Pain",
            "INJURY_FLG": "No",
            "NALOXONE_GIVEN_FLG": "0"
        },
        {
            "_source_row_num": 2,
            "INCIDENT_DT": "2024-01-15",
            "INCIDENT_COUNTY": "MARION",
            "CHIEF_COMPLAINT_DISPATCH": "Difficulty Breathing",
            "INJURY_FLG": "No",
            "NALOXONE_GIVEN_FLG": "0"
        }
    ]

    inserted = stage_records(test_records, "test_file.csv")
    print(f"Inserted: {inserted}")
    print(f"Total staged: {get_staging_count()}")

    print("\nSample records:")
    for record in get_staging_sample(limit=2):
        print(f"  {record['_source_row_num']}: {record['INCIDENT_DT']} - {record['INCIDENT_COUNTY']}")
