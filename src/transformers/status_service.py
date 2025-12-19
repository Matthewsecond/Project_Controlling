"""Business logic for status update operations."""
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.engine import Engine

from src.utilities import config
from src.utilities.models import StatusStats
from src.transformers import data_processor
from src.loaders import database

logger = logging.getLogger(__name__)


def process_status_updates(
    status_df: pd.DataFrame,
    existing_records: pd.DataFrame,
    engine: Engine,
) -> StatusStats:
    """
    Process status updates and update database.
    
    Compares new status values against existing records and applies
    updates or inserts as needed.
    
    Args:
        status_df: New status records to process
        existing_records: Existing records from database
        engine: Database engine
        
    Returns:
        Status update statistics
    """
    stats = StatusStats()
    
    if status_df.empty:
        return stats

    # Prepare working dataset
    working = status_df.copy()
    working["Date"] = pd.to_datetime(working["Date"], errors="coerce")
    working = working.dropna(subset=["Date", "Employee_Name", "Project_ID"])
    
    if working.empty:
        return stats

    # Clean status values
    working["Status"] = _clean_status_values(working["Status"])
    working = working[working["Status"] != ""]
    
    if working.empty:
        return stats

    # Deduplicate
    working = working.sort_values(["Date", "Employee_Name", "Project_ID"])
    working = working.drop_duplicates(subset=config.STATUS_DEDUP_COLUMNS, keep="last")

    # Map existing statuses
    if not existing_records.empty:
        existing_records["simple_key"] = data_processor.build_simple_key(existing_records)
        status_map = (
            existing_records
            .drop_duplicates(subset=["simple_key"], keep="last")
            .set_index("simple_key")["Status"]
            .fillna("")
        )
        working["simple_key"] = data_processor.build_simple_key(working)
        working["existing_status"] = _clean_status_values(
            working["simple_key"].map(status_map).fillna("")
        )
    else:
        working["simple_key"] = data_processor.build_simple_key(working)
        working["existing_status"] = ""

    # Identify updates and inserts
    updates = working[working["existing_status"] != ""]
    updates = updates[
        updates["existing_status"].astype(str).str.strip().str.lower()
        != updates["Status"].astype(str).str.strip().str.lower()
    ].copy()
    
    inserts = working[working["existing_status"] == ""].copy()

    # Apply changes
    try:
        # Update existing records
        if not updates.empty:
            num_updated = database.update_status_records(engine, updates)
            stats.updated = num_updated
            logger.info("Updated %d status records", stats.updated)

        # Insert new status records
        if not inserts.empty:
            insert_payload = pd.DataFrame({
                "Date": inserts["Date"],
                "Employee_Name": inserts["Employee_Name"],
                "Employee_Role": None,
                "Office_Location": None,
                "Project_Name": None,
                "Project_ID": inserts["Project_ID"],
                "Working_Hours": None,
                "Status": inserts["Status"],
                "Interviews": None,
                "Database": None,
                "Database_Converted": None,
            })

            insert_payload = data_processor.enforce_hours_status_rule(insert_payload)
            insert_payload = data_processor.format_for_mysql(insert_payload)
            
            database.bulk_insert_timesheets(engine, insert_payload, config.UNIONED_TABLE)
            stats.inserted = len(inserts)
            logger.info("Inserted %d new status records", stats.inserted)

    except Exception as exc:
        stats.errors.append(f"Error updating status information: {exc}")
        logger.exception("Failed to apply status updates")

    return stats


def _clean_status_values(series: pd.Series) -> pd.Series:
    """
    Clean and normalize status values.
    
    Args:
        series: Status series
        
    Returns:
        Cleaned series
    """
    if series is None:
        return pd.Series(dtype="string")
    
    original = series.astype("string").fillna("").str.strip()
    original = original.str.replace(r"[\r\n\t]+", "", regex=True)
    folded = original.str.lower()
    meaningful_mask = ~folded.isin(config.NON_MEANINGFUL_STATUS)
    return original.where(meaningful_mask, "")
