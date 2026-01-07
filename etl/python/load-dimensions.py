"""
Dimension Loading Module for EMS ETL Pipeline
Author: J. Ryan Butcher

Loads dimension tables with proper surrogate key handling and SCD logic.
"""

import sqlite3
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

from config import get_config


class DimensionLoader:
    """
    Handles loading of all dimension tables.

    Implements Type 1 SCD (overwrite) for most dimensions and
    maintains lookup caches for fast surrogate key resolution.
    """

    def __init__(self, db_path: str = None):
        """Initialize dimension loader."""
        config = get_config()
        self.db_path = db_path or config.database.sqlite_path

        # Lookup caches: natural_key -> surrogate_key
        self.county_cache: Dict[str, int] = {}
        self.complaint_cache: Dict[str, int] = {}
        self.anatomic_cache: Dict[str, int] = {}
        self.symptom_cache: Dict[str, int] = {}
        self.impression_cache: Dict[str, int] = {}
        self.disposition_cache: Dict[str, int] = {}
        self.destination_cache: Dict[str, int] = {}
        self.provider_org_cache: Dict[str, int] = {}
        self.service_level_cache: Dict[str, int] = {}

        # Initialize tables and caches
        self._init_dimension_tables()
        self._load_caches()

    def _get_connection(self):
        """Get database connection."""
        return sqlite3.connect(self.db_path)

    def _init_dimension_tables(self):
        """Create dimension tables if they don't exist."""
        conn = self._get_connection()
        try:
            conn.executescript("""
                -- Date dimension (will be pre-populated)
                CREATE TABLE IF NOT EXISTS DIM_DATE (
                    date_key INTEGER PRIMARY KEY,
                    date_value TEXT,
                    day_of_week TEXT,
                    day_of_week_num INTEGER,
                    day_of_month INTEGER,
                    month_num INTEGER,
                    month_name TEXT,
                    quarter_num INTEGER,
                    year_num INTEGER,
                    is_weekend INTEGER DEFAULT 0
                );

                -- Time of day dimension
                CREATE TABLE IF NOT EXISTS DIM_TIME_OF_DAY (
                    time_of_day_key INTEGER PRIMARY KEY,
                    hour_24 INTEGER,
                    hour_12 INTEGER,
                    minute_of_hour INTEGER,
                    am_pm TEXT,
                    time_period TEXT,
                    shift_name TEXT
                );

                -- County dimension
                CREATE TABLE IF NOT EXISTS DIM_COUNTY (
                    county_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    county_name TEXT UNIQUE NOT NULL,
                    state_code TEXT DEFAULT 'IN',
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Chief complaint dimension
                CREATE TABLE IF NOT EXISTS DIM_CHIEF_COMPLAINT (
                    chief_complaint_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    chief_complaint_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Anatomic location dimension
                CREATE TABLE IF NOT EXISTS DIM_ANATOMIC_LOCATION (
                    anatomic_location_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    anatomic_location_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Symptom dimension
                CREATE TABLE IF NOT EXISTS DIM_SYMPTOM (
                    symptom_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    symptom_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Provider impression dimension
                CREATE TABLE IF NOT EXISTS DIM_PROVIDER_IMPRESSION (
                    provider_impression_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    impression_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Disposition dimension (shared ED/Hospital)
                CREATE TABLE IF NOT EXISTS DIM_DISPOSITION (
                    disposition_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    disposition_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Destination type dimension
                CREATE TABLE IF NOT EXISTS DIM_DESTINATION_TYPE (
                    destination_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    destination_type_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Provider organization dimension
                CREATE TABLE IF NOT EXISTS DIM_PROVIDER_ORGANIZATION (
                    provider_org_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_structure TEXT NOT NULL,
                    provider_service TEXT,
                    org_lookup_key TEXT UNIQUE,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );

                -- Service level dimension
                CREATE TABLE IF NOT EXISTS DIM_SERVICE_LEVEL (
                    service_level_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_level_name TEXT UNIQUE NOT NULL,
                    _row_created_dt TEXT,
                    _row_updated_dt TEXT
                );
            """)
            conn.commit()
        finally:
            conn.close()

        # Seed unknown members
        self._seed_unknown_members()

        # Populate date and time dimensions
        self._populate_date_dimension()
        self._populate_time_dimension()

    def _seed_unknown_members(self):
        """Insert unknown member records with key = -1."""
        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()

            # Check if unknown members already exist
            cursor = conn.execute("SELECT county_key FROM DIM_COUNTY WHERE county_key = -1")
            if cursor.fetchone():
                return  # Already seeded

            conn.executescript(f"""
                INSERT OR IGNORE INTO DIM_DATE (date_key, date_value, day_of_week, year_num) VALUES (-1, '1900-01-01', 'Unknown', 1900);
                INSERT OR IGNORE INTO DIM_TIME_OF_DAY (time_of_day_key, hour_24, time_period) VALUES (-1, 0, 'Unknown');
                INSERT OR IGNORE INTO DIM_COUNTY (county_key, county_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_CHIEF_COMPLAINT (chief_complaint_key, chief_complaint_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_ANATOMIC_LOCATION (anatomic_location_key, anatomic_location_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_SYMPTOM (symptom_key, symptom_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_PROVIDER_IMPRESSION (provider_impression_key, impression_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_DISPOSITION (disposition_key, disposition_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_DESTINATION_TYPE (destination_type_key, destination_type_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_PROVIDER_ORGANIZATION (provider_org_key, provider_structure, org_lookup_key, _row_created_dt) VALUES (-1, 'Unknown', 'Unknown||Unknown', '{now}');
                INSERT OR IGNORE INTO DIM_SERVICE_LEVEL (service_level_key, service_level_name, _row_created_dt) VALUES (-1, 'Unknown', '{now}');
            """)
            conn.commit()
        finally:
            conn.close()

    def _populate_date_dimension(self):
        """Populate date dimension from 2014 to 2030."""
        conn = self._get_connection()
        try:
            # Check if already populated
            cursor = conn.execute("SELECT COUNT(*) FROM DIM_DATE WHERE date_key > 0")
            if cursor.fetchone()[0] > 0:
                return

            # Generate dates
            from datetime import date, timedelta

            start_date = date(2014, 1, 1)
            end_date = date(2030, 12, 31)
            current = start_date

            rows = []
            while current <= end_date:
                date_key = int(current.strftime("%Y%m%d"))
                rows.append((
                    date_key,
                    current.isoformat(),
                    current.strftime("%A"),
                    current.isoweekday(),
                    current.day,
                    current.month,
                    current.strftime("%B"),
                    (current.month - 1) // 3 + 1,
                    current.year,
                    1 if current.weekday() >= 5 else 0
                ))
                current += timedelta(days=1)

            conn.executemany("""
                INSERT OR IGNORE INTO DIM_DATE
                (date_key, date_value, day_of_week, day_of_week_num, day_of_month,
                 month_num, month_name, quarter_num, year_num, is_weekend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
        finally:
            conn.close()

    def _populate_time_dimension(self):
        """Populate time dimension (1440 minutes)."""
        conn = self._get_connection()
        try:
            # Check if already populated
            cursor = conn.execute("SELECT COUNT(*) FROM DIM_TIME_OF_DAY WHERE time_of_day_key >= 0")
            if cursor.fetchone()[0] > 0:
                return

            rows = []
            for minute in range(1440):
                hour24 = minute // 60
                hour12 = hour24 % 12 or 12
                min_of_hour = minute % 60
                am_pm = "AM" if hour24 < 12 else "PM"

                if hour24 < 5:
                    period = "Late Night"
                elif hour24 < 8:
                    period = "Early Morning"
                elif hour24 < 12:
                    period = "Morning"
                elif hour24 < 17:
                    period = "Afternoon"
                elif hour24 < 21:
                    period = "Evening"
                else:
                    period = "Night"

                if 7 <= hour24 < 15:
                    shift = "Day Shift"
                elif 15 <= hour24 < 23:
                    shift = "Swing Shift"
                else:
                    shift = "Night Shift"

                rows.append((minute, hour24, hour12, min_of_hour, am_pm, period, shift))

            conn.executemany("""
                INSERT OR IGNORE INTO DIM_TIME_OF_DAY
                (time_of_day_key, hour_24, hour_12, minute_of_hour, am_pm, time_period, shift_name)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
        finally:
            conn.close()

    def _load_caches(self):
        """Load existing dimension values into caches."""
        conn = self._get_connection()
        try:
            # County
            for row in conn.execute("SELECT county_key, county_name FROM DIM_COUNTY"):
                self.county_cache[row[1]] = row[0]

            # Chief complaint
            for row in conn.execute("SELECT chief_complaint_key, chief_complaint_name FROM DIM_CHIEF_COMPLAINT"):
                self.complaint_cache[row[1]] = row[0]

            # Anatomic location
            for row in conn.execute("SELECT anatomic_location_key, anatomic_location_name FROM DIM_ANATOMIC_LOCATION"):
                self.anatomic_cache[row[1]] = row[0]

            # Symptom
            for row in conn.execute("SELECT symptom_key, symptom_name FROM DIM_SYMPTOM"):
                self.symptom_cache[row[1]] = row[0]

            # Provider impression
            for row in conn.execute("SELECT provider_impression_key, impression_name FROM DIM_PROVIDER_IMPRESSION"):
                self.impression_cache[row[1]] = row[0]

            # Disposition
            for row in conn.execute("SELECT disposition_key, disposition_name FROM DIM_DISPOSITION"):
                self.disposition_cache[row[1]] = row[0]

            # Destination type
            for row in conn.execute("SELECT destination_type_key, destination_type_name FROM DIM_DESTINATION_TYPE"):
                self.destination_cache[row[1]] = row[0]

            # Provider organization
            for row in conn.execute("SELECT provider_org_key, org_lookup_key FROM DIM_PROVIDER_ORGANIZATION"):
                self.provider_org_cache[row[1]] = row[0]

            # Service level
            for row in conn.execute("SELECT service_level_key, service_level_name FROM DIM_SERVICE_LEVEL"):
                self.service_level_cache[row[1]] = row[0]

        finally:
            conn.close()

    def get_or_create_county(self, county_name: Optional[str]) -> int:
        """Get or create county dimension record."""
        if not county_name:
            return -1

        if county_name in self.county_cache:
            return self.county_cache[county_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_COUNTY (county_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (county_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.county_cache[county_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_complaint(self, complaint_name: Optional[str]) -> int:
        """Get or create chief complaint dimension record."""
        if not complaint_name:
            return -1

        if complaint_name in self.complaint_cache:
            return self.complaint_cache[complaint_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_CHIEF_COMPLAINT (chief_complaint_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (complaint_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.complaint_cache[complaint_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_anatomic(self, location_name: Optional[str]) -> int:
        """Get or create anatomic location dimension record."""
        if not location_name:
            return -1

        if location_name in self.anatomic_cache:
            return self.anatomic_cache[location_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_ANATOMIC_LOCATION (anatomic_location_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (location_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.anatomic_cache[location_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_symptom(self, symptom_name: Optional[str]) -> int:
        """Get or create symptom dimension record."""
        if not symptom_name:
            return -1

        if symptom_name in self.symptom_cache:
            return self.symptom_cache[symptom_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_SYMPTOM (symptom_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (symptom_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.symptom_cache[symptom_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_impression(self, impression_name: Optional[str]) -> int:
        """Get or create provider impression dimension record."""
        if not impression_name:
            return -1

        if impression_name in self.impression_cache:
            return self.impression_cache[impression_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_PROVIDER_IMPRESSION (impression_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (impression_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.impression_cache[impression_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_disposition(self, disposition_name: Optional[str]) -> int:
        """Get or create disposition dimension record."""
        if not disposition_name:
            return -1

        if disposition_name in self.disposition_cache:
            return self.disposition_cache[disposition_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_DISPOSITION (disposition_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (disposition_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.disposition_cache[disposition_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_destination(self, destination_name: Optional[str]) -> int:
        """Get or create destination type dimension record."""
        if not destination_name:
            return -1

        if destination_name in self.destination_cache:
            return self.destination_cache[destination_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_DESTINATION_TYPE (destination_type_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (destination_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.destination_cache[destination_name] = key
            return key
        finally:
            conn.close()

    def get_or_create_provider_org(
        self,
        structure: Optional[str],
        service: Optional[str]
    ) -> int:
        """Get or create provider organization dimension record."""
        if not structure:
            return -1

        lookup_key = f"{structure}||{service or ''}"

        if lookup_key in self.provider_org_cache:
            return self.provider_org_cache[lookup_key]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                """INSERT INTO DIM_PROVIDER_ORGANIZATION
                   (provider_structure, provider_service, org_lookup_key, _row_created_dt, _row_updated_dt)
                   VALUES (?, ?, ?, ?, ?)""",
                (structure, service, lookup_key, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.provider_org_cache[lookup_key] = key
            return key
        finally:
            conn.close()

    def get_or_create_service_level(self, level_name: Optional[str]) -> int:
        """Get or create service level dimension record."""
        if not level_name:
            return -1

        if level_name in self.service_level_cache:
            return self.service_level_cache[level_name]

        conn = self._get_connection()
        try:
            now = datetime.now().isoformat()
            cursor = conn.execute(
                "INSERT INTO DIM_SERVICE_LEVEL (service_level_name, _row_created_dt, _row_updated_dt) VALUES (?, ?, ?)",
                (level_name, now, now)
            )
            conn.commit()
            key = cursor.lastrowid
            self.service_level_cache[level_name] = key
            return key
        finally:
            conn.close()

    def get_dimension_counts(self) -> Dict[str, int]:
        """Get row counts for all dimension tables."""
        conn = self._get_connection()
        try:
            counts = {}
            tables = [
                "DIM_DATE", "DIM_TIME_OF_DAY", "DIM_COUNTY", "DIM_CHIEF_COMPLAINT",
                "DIM_ANATOMIC_LOCATION", "DIM_SYMPTOM", "DIM_PROVIDER_IMPRESSION",
                "DIM_DISPOSITION", "DIM_DESTINATION_TYPE", "DIM_PROVIDER_ORGANIZATION",
                "DIM_SERVICE_LEVEL"
            ]
            for table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            return counts
        finally:
            conn.close()


if __name__ == "__main__":
    # Test dimension loader
    loader = DimensionLoader()

    print("Dimension counts:")
    for table, count in loader.get_dimension_counts().items():
        print(f"  {table}: {count:,}")

    # Test lookups
    print("\nTest lookups:")
    print(f"  County 'LAKE': {loader.get_or_create_county('LAKE')}")
    print(f"  County 'MARION': {loader.get_or_create_county('MARION')}")
    print(f"  Complaint 'Chest Pain': {loader.get_or_create_complaint('Chest Pain')}")
