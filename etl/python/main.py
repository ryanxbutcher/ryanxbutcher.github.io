#!/usr/bin/env python3
"""
EMS Data Warehouse ETL Pipeline - Main Orchestrator
Author: J. Ryan Butcher

End-to-end ETL solution for loading EMS incident data into a Kimball-style
dimensional warehouse. Supports chunked processing for large datasets.

Usage:
    python main.py [--env dev|test|prod] [--file <path>] [--full|--incremental]

Example:
    python main.py --env dev --file ./data/input/ems_runs_2024.csv --full
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import get_config, Config
from extract import extract_csv_chunks, validate_source_file, find_source_files
from stage import init_staging_table, stage_records, get_staging_count
from transform import transform_record
from tqdm import tqdm


# Import dimension and fact loaders
try:
    # Try kebab-case imports (Python doesn't allow hyphens in module names)
    # So we use importlib
    import importlib.util

    def load_module(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    base_path = Path(__file__).parent
    load_dimensions = load_module("load_dimensions", base_path / "load-dimensions.py")
    load_facts = load_module("load_facts", base_path / "load-facts.py")
    logging_utils = load_module("logging_utils", base_path / "logging-utils.py")

    DimensionLoader = load_dimensions.DimensionLoader
    init_fact_table = load_facts.init_fact_table
    truncate_fact_table = load_facts.truncate_fact_table
    load_fact_batch = load_facts.load_fact_batch
    get_fact_count = load_facts.get_fact_count
    get_fact_summary = load_facts.get_fact_summary
    ETLLogger = logging_utils.ETLLogger

except Exception as e:
    print(f"Warning: Could not load modules with hyphens: {e}")
    print("Falling back to direct imports...")
    # Fallback for testing
    DimensionLoader = None
    ETLLogger = None


def run_etl(
    source_file: str,
    config: Config,
    full_refresh: bool = True
) -> bool:
    """
    Execute the full ETL pipeline.

    Args:
        source_file: Path to source CSV file
        config: Configuration object
        full_refresh: If True, truncate and reload; if False, incremental

    Returns:
        True if successful, False otherwise
    """
    db_path = config.database.sqlite_path

    # Ensure database directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Initialize logger
    if ETLLogger:
        logger = ETLLogger(db_path)
        run_id = logger.start_run(
            source_file=Path(source_file).name,
            environment=config.environment,
            load_type="full" if full_refresh else "incremental"
        )
    else:
        logger = None
        print(f"{'=' * 60}")
        print(f"ETL Pipeline Starting")
        print(f"Source: {source_file}")
        print(f"{'=' * 60}")

    try:
        # === Step 1: Validate Source ===
        print("\n[Step 1] Validating source file...")
        validation = validate_source_file(source_file)

        if not validation.success:
            raise ValueError(f"Source validation failed: {validation.error_message}")

        total_rows = validation.total_rows
        print(f"  Source file valid: {total_rows:,} rows, {len(validation.columns)} columns")

        # === Step 2: Initialize Staging ===
        print("\n[Step 2] Initializing staging table...")
        init_staging_table(db_path)
        print("  Staging table ready")

        # === Step 3: Extract and Stage ===
        print("\n[Step 3] Extracting and staging data...")
        staged_count = 0

        # Process in chunks with progress bar
        for chunk, start_row in tqdm(
            extract_csv_chunks(source_file, config.etl.batch_size),
            total=(total_rows // config.etl.batch_size) + 1,
            desc="  Staging"
        ):
            inserted = stage_records(chunk, Path(source_file).name, db_path)
            staged_count += inserted

        print(f"  Staged: {staged_count:,} records")

        # === Step 4: Initialize Warehouse ===
        print("\n[Step 4] Initializing warehouse tables...")

        if DimensionLoader:
            dim_loader = DimensionLoader(db_path)
        else:
            print("  Warning: DimensionLoader not available")
            dim_loader = None

        init_fact_table(db_path)

        if full_refresh:
            truncate_fact_table(db_path)
            print("  Fact table truncated for full refresh")

        # Get dimension counts
        if dim_loader:
            dim_counts = dim_loader.get_dimension_counts()
            print("  Dimension counts:")
            for table, count in dim_counts.items():
                print(f"    {table}: {count:,}")

        # === Step 5: Transform and Load ===
        print("\n[Step 5] Transforming and loading facts...")

        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Process staging in batches
        batch_size = config.etl.batch_size
        offset = 0
        fact_count = 0
        rejected_count = 0

        # Count staging records
        cursor = conn.execute("SELECT COUNT(*) FROM STG_EMS_INCIDENT")
        staging_total = cursor.fetchone()[0]

        with tqdm(total=staging_total, desc="  Loading") as pbar:
            while True:
                cursor = conn.execute(
                    f"SELECT * FROM STG_EMS_INCIDENT LIMIT {batch_size} OFFSET {offset}"
                )
                rows = cursor.fetchall()

                if not rows:
                    break

                # Transform and prepare fact records
                fact_records = []

                for row in rows:
                    record = dict(row)

                    # Transform the record
                    result = transform_record(record)

                    if not result.is_valid:
                        rejected_count += 1
                        continue

                    # Get dimension keys
                    cleaned = result.cleaned_data
                    derived = result.derived_data

                    fact_record = {
                        "date_key": derived["date_key"],
                        "time_of_day_key": derived["time_of_day_key"],
                        "county_key": dim_loader.get_or_create_county(cleaned["incident_county"]) if dim_loader else -1,
                        "chief_complaint_key": dim_loader.get_or_create_complaint(cleaned["chief_complaint_dispatch"]) if dim_loader else -1,
                        "anatomic_location_key": dim_loader.get_or_create_anatomic(cleaned["chief_complaint_anatomic_loc"]) if dim_loader else -1,
                        "symptom_key": dim_loader.get_or_create_symptom(cleaned["primary_symptom"]) if dim_loader else -1,
                        "provider_impression_key": dim_loader.get_or_create_impression(cleaned["provider_impression_primary"]) if dim_loader else -1,
                        "disposition_ed_key": dim_loader.get_or_create_disposition(cleaned["disposition_ed"]) if dim_loader else -1,
                        "disposition_hospital_key": dim_loader.get_or_create_disposition(cleaned["disposition_hospital"]) if dim_loader else -1,
                        "destination_type_key": dim_loader.get_or_create_destination(cleaned["destination_type"]) if dim_loader else -1,
                        "provider_org_key": dim_loader.get_or_create_provider_org(
                            cleaned["provider_type_structure"],
                            cleaned["provider_type_service"]
                        ) if dim_loader else -1,
                        "service_level_key": dim_loader.get_or_create_service_level(cleaned["provider_type_service_level"]) if dim_loader else -1,
                        "provider_to_scene_mins": cleaned["provider_to_scene_mins"],
                        "provider_to_dest_mins": cleaned["provider_to_dest_mins"],
                        "dispatch_to_arrival_mins": derived["dispatch_to_arrival_mins"],
                        "arrival_to_patient_mins": derived["arrival_to_patient_mins"],
                        "scene_time_mins": derived["scene_time_mins"],
                        "total_call_time_mins": derived["total_call_time_mins"],
                        "injury_flg": cleaned["injury_flg"],
                        "naloxone_given_flg": cleaned["naloxone_given_flg"],
                        "medication_given_flg": cleaned["medication_given_flg"],
                        "incident_count": derived["incident_count"],
                        "unit_notified_dt": cleaned["unit_notified_dt"],
                        "unit_arrived_scene_dt": cleaned["unit_arrived_scene_dt"],
                        "unit_arrived_patient_dt": cleaned["unit_arrived_patient_dt"],
                        "unit_left_scene_dt": cleaned["unit_left_scene_dt"],
                        "patient_arrived_dest_dt": cleaned["patient_arrived_dest_dt"],
                        "_source_row_num": cleaned["_source_row_num"]
                    }

                    fact_records.append(fact_record)

                # Batch insert facts
                if fact_records:
                    inserted = load_fact_batch(fact_records, Path(source_file).name, db_path)
                    fact_count += inserted

                pbar.update(len(rows))
                offset += batch_size

        conn.close()

        print(f"  Facts loaded: {fact_count:,}")
        print(f"  Rejected: {rejected_count:,}")

        # === Step 6: Verification ===
        print("\n[Step 6] Verification...")

        summary = get_fact_summary(db_path)
        print("  Fact table summary:")
        print(f"    Total incidents: {summary['total_incidents']:,}")
        print(f"    Injury incidents: {summary['injury_incidents']:,}")
        print(f"    Naloxone administrations: {summary['naloxone_incidents']:,}")
        print(f"    Avg response time: {summary['avg_response_mins']} minutes")
        print(f"    Date range: {summary['min_date_key']} to {summary['max_date_key']}")

        # Final dimension counts
        if dim_loader:
            print("\n  Final dimension counts:")
            for table, count in dim_loader.get_dimension_counts().items():
                print(f"    {table}: {count:,}")

        # End logging
        if logger:
            logger.end_run("SUCCESS", total_rows)

        print(f"\n{'=' * 60}")
        print("ETL Pipeline Completed Successfully!")
        print(f"{'=' * 60}")

        return True

    except Exception as e:
        error_msg = str(e)
        print(f"\nETL FAILED: {error_msg}")

        if logger:
            logger.end_run("FAILED", error_message=error_msg)

        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="EMS Data Warehouse ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --file ./data/input/ems_runs_2024.csv
  python main.py --env prod --full
  python main.py --file ./data/input/*.csv --incremental
        """
    )

    parser.add_argument(
        "--env",
        choices=["dev", "test", "prod"],
        default="dev",
        help="Environment (default: dev)"
    )

    parser.add_argument(
        "--file",
        type=str,
        help="Path to source CSV file (default: auto-detect in source_path)"
    )

    parser.add_argument(
        "--full",
        action="store_true",
        default=True,
        help="Full refresh (truncate and reload) - default"
    )

    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental load (append only)"
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to config.yaml file"
    )

    args = parser.parse_args()

    # Load configuration
    config_path = args.config
    if config_path is None:
        base_dir = Path(__file__).parent.parent.parent
        config_path = str(base_dir / "config" / "config.yaml")

    config = get_config(config_path, args.env)

    # Determine source file
    if args.file:
        source_file = args.file
    else:
        # Auto-detect CSV files in source path
        files = find_source_files("*.csv")
        if not files:
            print(f"No CSV files found in {config.etl.source_path}")
            sys.exit(1)
        source_file = files[0]
        print(f"Auto-detected source file: {source_file}")

    # Determine load type
    full_refresh = not args.incremental

    # Run ETL
    success = run_etl(source_file, config, full_refresh)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
