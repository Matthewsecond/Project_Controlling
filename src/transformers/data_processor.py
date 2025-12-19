"""Data processing, cleaning, and normalization for timesheet system."""
import logging
from typing import Sequence
import pandas as pd

from src.utilities import config

logger = logging.getLogger(__name__)


def normalize_text_series(series: pd.Series) -> pd.Series:
    """
    Normalize a text series by stripping whitespace.
    
    Args:
        series: Input series
        
    Returns:
        Normalized series
    """
    return series.apply(lambda value: "" if pd.isna(value) else str(value).strip())


def build_simple_key(df: pd.DataFrame) -> pd.Series:
    """
    Build a simple key from Date, Employee_Name, Project_ID.
    
    Args:
        df: DataFrame with required columns
        
    Returns:
        Series with keys
    """
    if df.empty:
        return pd.Series(dtype="string")
    
    work = df.copy()
    work["Date"] = pd.to_datetime(work["Date"], errors="coerce")
    
    return (
        work["Date"].dt.strftime("%Y-%m-%d").fillna("")
        + "|"
        + normalize_text_series(work.get("Employee_Name", pd.Series([], dtype="object")))
        + "|"
        + normalize_text_series(work.get("Project_ID", pd.Series([], dtype="object")))
    )


def build_signature(df: pd.DataFrame) -> pd.Series:
    """
    Build a signature from all relevant columns for change detection.
    
    Args:
        df: DataFrame with required columns
        
    Returns:
        Series with signatures
    """
    if df.empty:
        return pd.Series(dtype="string")
    
    work = df.copy()
    work["Date"] = pd.to_datetime(work["Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    
    signature_parts = [work["Date"]]
    for column in config.SIGNATURE_EXTRA_COLUMNS:
        signature_parts.append(
            normalize_text_series(work.get(column, pd.Series([], dtype="object")))
        )
    
    combined = pd.concat(signature_parts, axis=1)
    return combined.agg("|".join, axis=1)


def enforce_hours_status_rule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep rows only if:
      - Working_Hours > 0  OR
      - Status is meaningful.
    If Status is meaningful but hours <= 0/NaN, force Working_Hours -> NULL.
    
    Args:
        df: Input dataframe
        
    Returns:
        Filtered dataframe
    """
    if df is None or df.empty:
        return df
    
    work = df.copy()
    work["Status"] = work.get("Status", pd.Series([], dtype="object"))
    work["Status"] = work["Status"].astype("string").fillna("").str.strip()
    
    hours = pd.to_numeric(
        work.get("Working_Hours", pd.Series([], dtype="float")), 
        errors="coerce"
    )
    
    keep_mask = (hours > 0) | (work["Status"] != "")
    work.loc[(work["Status"] != "") & (~(hours > 0)), "Working_Hours"] = pd.NA
    
    dropped = len(work) - int(keep_mask.sum())
    if dropped:
        logger.info(
            "Rule: dropped %s row(s) with 0/NULL hours and non-meaningful Status", 
            dropped
        )
    
    return work[keep_mask]


def format_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format dataframe for MySQL CSV import.
    - Dates -> 'YYYY-MM-DD'
    - Numeric columns -> format nicely, NULLs as \\N
    - Text columns -> fill missing with 'unknown'
    
    Args:
        df: Input dataframe
        
    Returns:
        Formatted dataframe
    """
    out = df.copy()

    # Format dates
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Format numeric columns
    for col in out.columns:
        if col in config.NUMERIC_COLUMNS:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            out[col] = out[col].apply(
                lambda x: ("%.2f" % x).rstrip("0").rstrip(".") if pd.notna(x) else "\\N"
            )

    # Format text columns
    for col in out.columns:
        if col in config.TEXT_COLUMNS:
            out[col] = out[col].astype("string")
            out[col] = out[col].str.strip()
            out[col] = out[col].where(
                out[col].notna() & (out[col] != ""), 
                config.UNKNOWN_TEXT
            )
        elif pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].astype("string").str.strip()

    return out


def deduplicate_dataframe(
    df: pd.DataFrame, 
    subset: Sequence[str], 
    label: str = "",
) -> pd.DataFrame:
    """
    Remove duplicate rows from a dataframe.
    
    Args:
        df: Input dataframe
        subset: Columns to use for duplicate detection
        label: Label for logging
        
    Returns:
        Deduplicated dataframe
    """
    if df.empty:
        return df
    
    usable_columns = [col for col in subset if col in df.columns]
    if not usable_columns:
        return df
    
    before = len(df)
    result = df.drop_duplicates(subset=usable_columns, keep="last")
    removed = before - len(result)
    
    if label and removed > 0:
        logger.info("%s: removed %s duplicate rows", label, removed)
    
    return result


def prepare_timesheet_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare timesheet dataframe for database insert.
    
    Args:
        df: Raw timesheet dataframe
        
    Returns:
        Cleaned and formatted dataframe
    """
    if df.empty:
        return df
    
    # Ensure all required columns exist
    for col in config.TIMESHEET_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Select only needed columns in correct order
    df = df[config.TIMESHEET_COLUMNS]
    
    # Apply business rules
    df = enforce_hours_status_rule(df)
    
    # Format for MySQL
    df = format_for_mysql(df)
    
    return df


def prepare_emails_for_update(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare email dataframe for database update.
    
    Args:
        df: Raw email dataframe
        
    Returns:
        Cleaned dataframe
    """
    if df.empty:
        return df
    
    working = df.rename(columns={"Employee Name": "Employee_Name", "Mail": "Mail"}).copy()
    working["Employee_Name"] = working["Employee_Name"].astype(str).str.strip()
    working["Mail"] = working["Mail"].astype(str).str.strip()
    working = working.dropna(subset=["Employee_Name", "Mail"])
    working = working[(working["Employee_Name"] != "") & (working["Mail"] != "")]
    
    return working
