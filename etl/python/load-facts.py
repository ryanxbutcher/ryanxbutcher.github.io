"""
Fact Loading Module for EMS ETL Pipeline
Author: J. Ryan Butcher

Loads fact table with proper dimension key lookups and batch processing.
"""

import sqlite3
from typing import List, Dict, Any
from datetime import datetime

from config import get_config


def init_fact_table(db_path: str = None):
    """
    Create fact table if it doesn't exist.

    Args:
        db_path: Path to SQLite database
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS FACT_EMS_INCIDENT (
                ems_incident_key INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Dimension foreign keys
                date_key INTEGER NOT NULL,
                time_of_day_key INTEGER NOT NULL,
                county_key INTEGER NOT NULL,
                chief_complaint_key INTEGER NOT NULL,
                anatomic_location_key INTEGER NOT NULL,
                symptom_key INTEGER NOT NULL,
                provider_impression_key INTEGER NOT NULL,
                disposition_ed_key INTEGER NOT NULL,
                disposition_hospital_key INTEGER NOT NULL,
                destination_type_key INTEGER NOT NULL,
                provider_org_key INTEGER NOT NULL,
                service_level_key INTEGER NOT NULL,

                -- Measures
                provider_to_scene_mins REAL,
                provider_to_dest_mins REAL,
                dispatch_to_arrival_mins REAL,
                arrival_to_patient_mins REAL,
                scene_time_mins REAL,
                total_call_time_mins REAL,

                -- Flags
                injury_flg INTEGER DEFAULT 0,
                naloxone_given_flg INTEGER DEFAULT 0,
                medication_given_flg INTEGER DEFAULT 0,
                incident_count INTEGER DEFAULT 1,

                -- Event timestamps
                unit_notified_dt TEXT,
                unit_arrived_scene_dt TEXT,
                unit_arrived_patient_dt TEXT,
                unit_left_scene_dt TEXT,
                patient_arrived_dest_dt TEXT,

                -- Audit
                _source_file TEXT,
                _source_row_num INTEGER,
                _row_created_dt TEXT
            )
        """)

        # Create indexes
        conn.execute("""
            CREATE INDEX IF NOT EXISTS IX_FACT_EMS_DATE
            ON FACT_EMS_INCIDENT (date_key)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS IX_FACT_EMS_COUNTY
            ON FACT_EMS_INCIDENT (county_key, date_key)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS IX_FACT_EMS_PROVIDER
            ON FACT_EMS_INCIDENT (provider_org_key, date_key)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS IX_FACT_EMS_NALOXONE
            ON FACT_EMS_INCIDENT (naloxone_given_flg, date_key)
        """)

        conn.commit()
    finally:
        conn.close()


def truncate_fact_table(db_path: str = None):
    """
    Truncate fact table for full refresh.

    Args:
        db_path: Path to SQLite database
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM FACT_EMS_INCIDENT")
        conn.commit()
    finally:
        conn.close()


def load_fact_batch(
    records: List[Dict[str, Any]],
    source_file: str,
    db_path: str = None
) -> int:
    """
    Load a batch of records into the fact table.

    Args:
        records: List of dictionaries with dimension keys and measures
        source_file: Source file name for audit
        db_path: Path to SQLite database

    Returns:
        Number of records inserted
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    if not records:
        return 0

    now = datetime.now().isoformat()

    insert_sql = """
        INSERT INTO FACT_EMS_INCIDENT (
            date_key, time_of_day_key, county_key, chief_complaint_key,
            anatomic_location_key, symptom_key, provider_impression_key,
            disposition_ed_key, disposition_hospital_key, destination_type_key,
            provider_org_key, service_level_key,
            provider_to_scene_mins, provider_to_dest_mins,
            dispatch_to_arrival_mins, arrival_to_patient_mins,
            scene_time_mins, total_call_time_mins,
            injury_flg, naloxone_given_flg, medication_given_flg, incident_count,
            unit_notified_dt, unit_arrived_scene_dt, unit_arrived_patient_dt,
            unit_left_scene_dt, patient_arrived_dest_dt,
            _source_file, _source_row_num, _row_created_dt
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?
        )
    """

    rows = []
    for record in records:
        row = (
            record.get("date_key", -1),
            record.get("time_of_day_key", -1),
            record.get("county_key", -1),
            record.get("chief_complaint_key", -1),
            record.get("anatomic_location_key", -1),
            record.get("symptom_key", -1),
            record.get("provider_impression_key", -1),
            record.get("disposition_ed_key", -1),
            record.get("disposition_hospital_key", -1),
            record.get("destination_type_key", -1),
            record.get("provider_org_key", -1),
            record.get("service_level_key", -1),
            record.get("provider_to_scene_mins"),
            record.get("provider_to_dest_mins"),
            record.get("dispatch_to_arrival_mins"),
            record.get("arrival_to_patient_mins"),
            record.get("scene_time_mins"),
            record.get("total_call_time_mins"),
            record.get("injury_flg", 0),
            record.get("naloxone_given_flg", 0),
            record.get("medication_given_flg", 0),
            record.get("incident_count", 1),
            record.get("unit_notified_dt"),
            record.get("unit_arrived_scene_dt"),
            record.get("unit_arrived_patient_dt"),
            record.get("unit_left_scene_dt"),
            record.get("patient_arrived_dest_dt"),
            source_file,
            record.get("_source_row_num"),
            now
        )
        rows.append(row)

    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(insert_sql, rows)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def get_fact_count(db_path: str = None) -> int:
    """
    Get count of records in fact table.

    Args:
        db_path: Path to SQLite database

    Returns:
        Number of fact records
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM FACT_EMS_INCIDENT")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def get_fact_summary(db_path: str = None) -> Dict[str, Any]:
    """
    Get summary statistics from fact table.

    Args:
        db_path: Path to SQLite database

    Returns:
        Dictionary with summary statistics
    """
    config = get_config()
    db_path = db_path or config.database.sqlite_path

    conn = sqlite3.connect(db_path)
    try:
        summary = {}

        # Total count
        cursor = conn.execute("SELECT COUNT(*) FROM FACT_EMS_INCIDENT")
        summary["total_incidents"] = cursor.fetchone()[0]

        # Injury count
        cursor = conn.execute("SELECT SUM(injury_flg) FROM FACT_EMS_INCIDENT")
        summary["injury_incidents"] = cursor.fetchone()[0] or 0

        # Naloxone count
        cursor = conn.execute("SELECT SUM(naloxone_given_flg) FROM FACT_EMS_INCIDENT")
        summary["naloxone_incidents"] = cursor.fetchone()[0] or 0

        # Average response time
        cursor = conn.execute("""
            SELECT AVG(provider_to_scene_mins)
            FROM FACT_EMS_INCIDENT
            WHERE provider_to_scene_mins IS NOT NULL AND provider_to_scene_mins > 0
        """)
        summary["avg_response_mins"] = round(cursor.fetchone()[0] or 0, 2)

        # Date range
        cursor = conn.execute("""
            SELECT MIN(date_key), MAX(date_key)
            FROM FACT_EMS_INCIDENT
            WHERE date_key > 0
        """)
        row = cursor.fetchone()
        summary["min_date_key"] = row[0]
        summary["max_date_key"] = row[1]

        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    # Test fact loading
    print("Initializing fact table...")
    init_fact_table()

    # Test with sample data
    test_records = [
        {
            "date_key": 20240115,
            "time_of_day_key": 870,  # 14:30
            "county_key": 1,
            "chief_complaint_key": 1,
            "anatomic_location_key": -1,
            "symptom_key": -1,
            "provider_impression_key": -1,
            "disposition_ed_key": -1,
            "disposition_hospital_key": -1,
            "destination_type_key": -1,
            "provider_org_key": 1,
            "service_level_key": 1,
            "provider_to_scene_mins": 5.5,
            "injury_flg": 0,
            "naloxone_given_flg": 0,
            "incident_count": 1,
            "_source_row_num": 1
        }
    ]

    inserted = load_fact_batch(test_records, "test_file.csv")
    print(f"Inserted: {inserted}")

    print("\nFact summary:")
    for key, value in get_fact_summary().items():
        print(f"  {key}: {value}")
