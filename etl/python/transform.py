"""
Data Transformation Module for EMS ETL Pipeline
Author: J. Ryan Butcher

Applies data quality rules, creates derived columns, and prepares data for loading.
"""

import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass

from config import get_config


@dataclass
class TransformResult:
    """Result of transformation for a single record."""
    cleaned_data: Dict[str, Any]
    derived_data: Dict[str, Any]
    errors: List[Tuple[str, str, str]]  # (column, error_type, message)
    is_valid: bool


def calculate_time_diff_minutes(
    start_dt: Optional[str],
    end_dt: Optional[str]
) -> Optional[float]:
    """
    Calculate time difference in minutes between two datetime strings.

    Args:
        start_dt: Start datetime string
        end_dt: End datetime string

    Returns:
        Minutes difference or None if calculation not possible
    """
    if not start_dt or not end_dt:
        return None

    try:
        # Handle date-only values
        formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]

        start = None
        end = None

        for fmt in formats:
            try:
                if start is None:
                    start = datetime.strptime(str(start_dt).strip(), fmt)
            except ValueError:
                pass
            try:
                if end is None:
                    end = datetime.strptime(str(end_dt).strip(), fmt)
            except ValueError:
                pass

        if start and end:
            diff = (end - start).total_seconds() / 60
            return round(diff, 2) if diff >= 0 else None

        return None

    except Exception:
        return None


def create_date_key(date_str: Optional[str]) -> int:
    """
    Create date dimension key (YYYYMMDD format).

    Args:
        date_str: Date string

    Returns:
        Integer date key or -1 for unknown
    """
    if not date_str:
        return -1

    try:
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]:
            try:
                dt = datetime.strptime(str(date_str).strip()[:10], fmt)
                return int(dt.strftime("%Y%m%d"))
            except ValueError:
                continue
        return -1
    except Exception:
        return -1


def create_time_key(datetime_str: Optional[str]) -> int:
    """
    Create time-of-day dimension key (minutes from midnight).

    Args:
        datetime_str: Datetime string

    Returns:
        Integer time key (0-1439) or -1 for unknown
    """
    if not datetime_str:
        return -1

    try:
        dt_str = str(datetime_str).strip()

        # If date-only, return midnight
        if len(dt_str) == 10:
            return 0

        # Try parsing with time component
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"]:
            try:
                dt = datetime.strptime(dt_str, fmt)
                return dt.hour * 60 + dt.minute
            except ValueError:
                continue

        return -1

    except Exception:
        return -1


def clean_text(value: Optional[str]) -> Optional[str]:
    """Clean and normalize text value."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def parse_flag(value: Any, yes_values: set = None) -> int:
    """Parse various flag formats to 0/1."""
    if value is None:
        return 0

    if yes_values is None:
        yes_values = {"yes", "y", "true", "1", "1.0"}

    if isinstance(value, (int, float)):
        return 1 if value else 0

    str_val = str(value).strip().lower()
    return 1 if str_val in yes_values else 0


def transform_record(record: Dict[str, Any]) -> TransformResult:
    """
    Transform a single staging record.

    Args:
        record: Raw staging record

    Returns:
        TransformResult with cleaned data, derived columns, and errors
    """
    errors = []
    cleaned = {}
    derived = {}

    # === Clean source columns ===

    # Date fields
    cleaned["incident_dt"] = clean_text(record.get("INCIDENT_DT"))

    # Text fields - clean and normalize
    cleaned["incident_county"] = clean_text(record.get("INCIDENT_COUNTY"))
    cleaned["chief_complaint_dispatch"] = clean_text(record.get("CHIEF_COMPLAINT_DISPATCH"))
    cleaned["chief_complaint_anatomic_loc"] = clean_text(record.get("CHIEF_COMPLAINT_ANATOMIC_LOC"))
    cleaned["primary_symptom"] = clean_text(record.get("PRIMARY_SYMPTOM"))
    cleaned["provider_impression_primary"] = clean_text(record.get("PROVIDER_IMPRESSION_PRIMARY"))
    cleaned["disposition_ed"] = clean_text(record.get("DISPOSITION_ED"))
    cleaned["disposition_hospital"] = clean_text(record.get("DISPOSITION_HOSPITAL"))
    cleaned["destination_type"] = clean_text(record.get("DESTINATION_TYPE"))
    cleaned["provider_type_structure"] = clean_text(record.get("PROVIDER_TYPE_STRUCTURE"))
    cleaned["provider_type_service"] = clean_text(record.get("PROVIDER_TYPE_SERVICE"))
    cleaned["provider_type_service_level"] = clean_text(record.get("PROVIDER_TYPE_SERVICE_LEVEL"))

    # Flag fields
    cleaned["injury_flg"] = parse_flag(record.get("INJURY_FLG"), {"yes", "y", "true", "1"})
    cleaned["naloxone_given_flg"] = parse_flag(record.get("NALOXONE_GIVEN_FLG"))
    cleaned["medication_given_flg"] = parse_flag(record.get("MEDICATION_GIVEN_OTHER_FLG"))

    # Numeric fields
    try:
        mins = record.get("PROVIDER_TO_SCENE_MINS")
        cleaned["provider_to_scene_mins"] = float(mins) if mins and str(mins).strip() else None
        if cleaned["provider_to_scene_mins"] and cleaned["provider_to_scene_mins"] < 0:
            cleaned["provider_to_scene_mins"] = None
    except (ValueError, TypeError):
        cleaned["provider_to_scene_mins"] = None

    try:
        mins = record.get("PROVIDER_TO_DESTINATION_MINS")
        cleaned["provider_to_dest_mins"] = float(mins) if mins and str(mins).strip() else None
        if cleaned["provider_to_dest_mins"] and cleaned["provider_to_dest_mins"] < 0:
            cleaned["provider_to_dest_mins"] = None
    except (ValueError, TypeError):
        cleaned["provider_to_dest_mins"] = None

    # Datetime fields
    cleaned["unit_notified_dt"] = clean_text(record.get("UNIT_NOTIFIED_BY_DISPATCH_DT"))
    cleaned["unit_arrived_scene_dt"] = clean_text(record.get("UNIT_ARRIVED_ON_SCENE_DT"))
    cleaned["unit_arrived_patient_dt"] = clean_text(record.get("UNIT_ARRIVED_TO_PATIENT_DT"))
    cleaned["unit_left_scene_dt"] = clean_text(record.get("UNIT_LEFT_SCENE_DT"))
    cleaned["patient_arrived_dest_dt"] = clean_text(record.get("PATIENT_ARRIVED_DESTINATION_DT"))

    # === Create derived columns ===

    # Date and time keys
    derived["date_key"] = create_date_key(cleaned["incident_dt"])
    derived["time_of_day_key"] = create_time_key(cleaned["unit_notified_dt"])

    # Calculated time measures
    derived["dispatch_to_arrival_mins"] = calculate_time_diff_minutes(
        cleaned["unit_notified_dt"],
        cleaned["unit_arrived_scene_dt"]
    )

    derived["arrival_to_patient_mins"] = calculate_time_diff_minutes(
        cleaned["unit_arrived_scene_dt"],
        cleaned["unit_arrived_patient_dt"]
    )

    derived["scene_time_mins"] = calculate_time_diff_minutes(
        cleaned["unit_arrived_scene_dt"],
        cleaned["unit_left_scene_dt"]
    )

    derived["total_call_time_mins"] = calculate_time_diff_minutes(
        cleaned["unit_notified_dt"],
        cleaned["patient_arrived_dest_dt"]
    )

    # Always 1 for counting
    derived["incident_count"] = 1

    # Source tracking
    cleaned["_source_row_num"] = record.get("_source_row_num")
    cleaned["_source_file"] = record.get("_source_file")

    # === Validation ===

    # Check required field
    if derived["date_key"] == -1:
        errors.append(("INCIDENT_DT", "INVALID_DATE", f"Cannot parse date: {record.get('INCIDENT_DT')}"))

    is_valid = len([e for e in errors if e[0] == "INCIDENT_DT"]) == 0

    return TransformResult(
        cleaned_data=cleaned,
        derived_data=derived,
        errors=errors,
        is_valid=is_valid
    )


def transform_batch(
    records: List[Dict[str, Any]]
) -> Tuple[List[TransformResult], int, int]:
    """
    Transform a batch of records.

    Args:
        records: List of staging records

    Returns:
        Tuple of (results, valid_count, rejected_count)
    """
    results = []
    valid_count = 0
    rejected_count = 0

    for record in records:
        result = transform_record(record)
        results.append(result)

        if result.is_valid:
            valid_count += 1
        else:
            rejected_count += 1

    return results, valid_count, rejected_count


if __name__ == "__main__":
    # Test transformation
    test_record = {
        "INCIDENT_DT": "2024-01-15",
        "INCIDENT_COUNTY": "  LAKE  ",
        "CHIEF_COMPLAINT_DISPATCH": "Chest Pain",
        "INJURY_FLG": "No",
        "NALOXONE_GIVEN_FLG": "1",
        "PROVIDER_TO_SCENE_MINS": "5.5",
        "UNIT_NOTIFIED_BY_DISPATCH_DT": "2024-01-15 14:30:00",
        "UNIT_ARRIVED_ON_SCENE_DT": "2024-01-15 14:38:00",
        "_source_row_num": 1
    }

    result = transform_record(test_record)
    print("Cleaned Data:")
    for k, v in result.cleaned_data.items():
        if v is not None:
            print(f"  {k}: {v}")

    print("\nDerived Data:")
    for k, v in result.derived_data.items():
        print(f"  {k}: {v}")

    print(f"\nValid: {result.is_valid}")
    print(f"Errors: {result.errors}")
