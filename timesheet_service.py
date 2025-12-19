"""Business logic for timesheet operations."""
import logging
from datetime import datetime
from typing import List

import pandas as pd
from sqlalchemy.engine import Engine

import config
import data_processor
import database
from models import ProcessingStats

logger = logging.getLogger(__name__)


def process_timesheet_changes(
    new_records: pd.DataFrame,
    existing_records: pd.DataFrame,
    engine: Engine,
) -> ProcessingStats:
    """
    Process timesheet changes and update database.
    
    Compares new records against existing records, identifies inserts and updates,
    and persists changes to database.
    
    Args:
        new_records: New timesheet records to process
        existing_records: Existing records from database
        engine: Database engine
        
    Returns:
        Processing statistics
    """
    stats = ProcessingStats()
    
    if new_records.empty:
        return stats

    # Prepare working dataset
    working = new_records.copy()
    working["Date"] = pd.to_datetime(working["Date"], errors="coerce")
    working = working.dropna(subset=["Date", "Employee_Name", "Project_ID"])
    
    if working.empty:
        stats.errors.append("No valid timesheet rows remained after cleaning.")
        return stats

    # Build keys for comparison
    working["simple_key"] = data_processor.build_simple_key(working)
    working["signature"] = data_processor.build_signature(working)

    # Map existing signatures
    if not existing_records.empty:
        # Add keys to existing records
        existing_records["simple_key"] = data_processor.build_simple_key(existing_records)
        existing_records["signature"] = data_processor.build_signature(existing_records)
        
        existing_map = (
            existing_records
            .sort_values(["simple_key", "Date"], ascending=[True, True], na_position="last")
            .drop_duplicates(subset=["simple_key"], keep="last")
            .set_index("simple_key")["signature"]
        )
        working["existing_signature"] = working["simple_key"].map(existing_map)
    else:
        working["existing_signature"] = pd.NA

    # Identify inserts and updates
    to_insert = working[working["existing_signature"].isna()].copy()
    to_update = working[
        working["existing_signature"].notna() 
        & (working["existing_signature"] != working["signature"])
    ].copy()
    
    stats.skipped = len(working) - len(to_insert) - len(to_update)

    if to_insert.empty and to_update.empty:
        logger.info("No timesheet changes to apply")
        return stats

    # Apply changes
    try:
        # Delete records that need updating
        if not to_update.empty:
            delete_keys = to_update[["Date", "Employee_Name", "Project_ID"]].drop_duplicates()
            database.delete_timesheet_records(engine, delete_keys)
            stats.updated = len(to_update)
            logger.info("Marked %d records for update", stats.updated)

        # Prepare all records for insert (both new and updated)
        frames_to_insert: List[pd.DataFrame] = []
        
        if not to_insert.empty:
            frames_to_insert.append(to_insert)
            stats.inserted = len(to_insert)
            logger.info("Prepared %d new records for insert", stats.inserted)
        
        if not to_update.empty:
            frames_to_insert.append(to_update)

        # Bulk insert
        if frames_to_insert:
            payload = pd.concat(frames_to_insert, ignore_index=True)
            payload = payload.drop(
                columns=["simple_key", "signature", "existing_signature"], 
                errors="ignore"
            )
            payload = data_processor.prepare_timesheet_for_insert(payload)
            
            database.bulk_insert_timesheets(engine, payload, config.UNIONED_TABLE)
            logger.info(
                "Successfully persisted timesheet changes: %d inserted, %d updated, %d skipped",
                stats.inserted,
                stats.updated,
                stats.skipped,
            )

    except Exception as exc:
        stats.errors.append(f"Error inserting or updating timesheet data: {exc}")
        logger.exception("Failed to persist timesheet data")

    return stats
