"""Excel file reading and validation for timesheet system."""
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import pandas as pd

from src.utilities import config
from src.utilities.models import DateWindow, TimesheetData

logger = logging.getLogger(__name__)


def find_excel_files(
    folder_path: str | Path,
    excluded_folders: Optional[Sequence[str]] = None,
) -> List[Path]:
    """
    Find all Excel files in folder, excluding specified folders.
    
    Args:
        folder_path: Path to search
        excluded_folders: Folders to skip
        
    Returns:
        List of Excel file paths
    """
    base_path = Path(folder_path)
    exclusions = {folder.lower() for folder in (excluded_folders or config.EXCLUDED_FOLDERS)}
    files: List[Path] = []
    
    if not base_path.exists():
        logger.warning("Folder %s does not exist", base_path)
        return files
    
    for path in base_path.rglob("*.xls*"):
        if path.name.startswith("~$"):
            continue
        if any(part.lower() in exclusions for part in path.parts):
            continue
        files.append(path)
    
    return files


def load_timesheet_files(
    folder_paths: Sequence[str | Path],
    window: DateWindow,
) -> TimesheetData:
    """
    Load timesheet data from multiple folders.
    
    Args:
        folder_paths: Folders to search for Excel files
        window: Date range to filter data
        
    Returns:
        TimesheetData with loaded data and errors
    """
    data = TimesheetData()
    timesheet_frames: List[pd.DataFrame] = []
    status_frames: List[pd.DataFrame] = []
    email_frames: List[pd.DataFrame] = []

    for folder in folder_paths:
        excel_files = find_excel_files(folder)
        if not excel_files:
            data.errors.append(f"No Excel files found in directory: {folder}")
            continue

        for file_path in excel_files:
            data.files_processed += 1
            
            # Open workbook
            try:
                workbook = pd.ExcelFile(file_path)
            except Exception as exc:
                data.errors.append(f"File {file_path} could not be opened: {exc}")
                continue

            # Read Database sheet
            if "Database" not in workbook.sheet_names:
                data.errors.append(f"File {file_path} is missing the 'Database' sheet")
                continue

            try:
                raw_database = workbook.parse("Database")
            except Exception as exc:
                data.errors.append(f"File {file_path} could not read 'Database' sheet: {exc}")
                continue

            # Process timesheets and statuses
            timesheet_df, status_df, file_errors = _process_database_sheet(
                raw_database, window, Path(file_path)
            )
            data.errors.extend(file_errors)

            if not timesheet_df.empty:
                timesheet_frames.append(timesheet_df)

            if not status_df.empty:
                status_df = status_df.copy()
                status_df["source_file"] = Path(file_path).name
                status_frames.append(status_df)

            # Read Employee sheet for emails
            if "Employee" in workbook.sheet_names:
                try:
                    employee_df = workbook.parse("Employee", usecols=config.EMPLOYEE_EMAIL_COLUMNS)
                except Exception as exc:
                    data.errors.append(f"File {file_path} has an 'Employee' sheet issue: {exc}")
                else:
                    if not employee_df.empty:
                        email_frames.append(employee_df)

    # Combine all frames
    data.timesheets = _concat_frames(timesheet_frames)
    data.statuses = _concat_frames(status_frames)
    data.emails = _concat_frames(email_frames)
    
    return data


def _process_database_sheet(
    df: pd.DataFrame,
    window: DateWindow,
    file_path: Path,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    Process a single Database sheet into timesheets and statuses.
    
    Args:
        df: Raw dataframe from Database sheet
        window: Date range to filter
        file_path: Source file path (for error messages)
        
    Returns:
        Tuple of (timesheet_df, status_df, errors)
    """
    errors: List[str] = []
    
    if df is None or df.empty:
        errors.append(f"File {file_path} has an empty 'Database' sheet")
        empty_timesheet = pd.DataFrame(columns=config.TIMESHEET_COLUMNS)
        empty_status = pd.DataFrame(columns=config.STATUS_COLUMNS)
        return empty_timesheet, empty_status, errors

    frame = df.copy()
    
    # Check required columns
    missing_required = [
        column for column in config.REQUIRED_CORE_COLUMNS 
        if column not in frame.columns
    ]
    if missing_required:
        errors.append(
            f"File {file_path} is missing required columns: {', '.join(missing_required)}"
        )

    # Ensure and rename columns
    for column in config.COLUMN_MAP.keys():
        if column not in frame.columns:
            frame[column] = pd.NA
    
    frame = frame.rename(columns=config.COLUMN_MAP)
    
    for column in config.TIMESHEET_COLUMNS + ["Working_Hours_Converted"]:
        if column not in frame.columns:
            frame[column] = pd.NA

    # Parse dates
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    invalid_dates = frame["Date"].isna()
    if invalid_dates.any():
        sample_indexes = invalid_dates[invalid_dates].index[:3]
        for idx in sample_indexes:
            errors.append(f"File {file_path} has invalid date in row {idx + 2}")
        if invalid_dates.sum() > 3:
            errors.append(
                f"File {file_path} has {invalid_dates.sum() - 3} more rows with invalid dates"
            )
        frame = frame[~invalid_dates]

    # Convert numeric columns
    for column in config.NUMERIC_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    # Use converted hours if main hours are missing
    if "Working_Hours_Converted" in frame.columns:
        frame["Working_Hours"] = frame["Working_Hours"].fillna(frame["Working_Hours_Converted"])

    # Filter by date window
    mask = (frame["Date"] >= pd.Timestamp(window.start)) & (frame["Date"] <= pd.Timestamp(window.end))
    frame = frame[mask]
    
    if frame.empty:
        return (
            pd.DataFrame(columns=config.TIMESHEET_COLUMNS),
            pd.DataFrame(columns=config.STATUS_COLUMNS),
            errors,
        )

    # Extract status data
    status_df = frame[config.STATUS_COLUMNS].copy()
    status_df["Status"] = _clean_status_column(status_df["Status"])
    status_df = status_df[status_df["Status"] != ""]

    # Extract timesheet data (only rows with hours > 0)
    timesheet_df = frame.copy()
    timesheet_df = timesheet_df[timesheet_df["Working_Hours"].fillna(0) > 0]
    timesheet_df = timesheet_df.drop(columns=["Working_Hours_Converted"], errors="ignore")
    timesheet_df = timesheet_df[config.TIMESHEET_COLUMNS]

    return timesheet_df, status_df, errors


def _clean_status_column(series: pd.Series) -> pd.Series:
    """Clean and normalize status column."""
    if series is None:
        return pd.Series(dtype="string")
    
    original = series.astype("string").fillna("").str.strip()
    original = original.str.replace(r"[\r\n\t]+", "", regex=True)
    folded = original.str.lower()
    meaningful_mask = ~folded.isin(config.NON_MEANINGFUL_STATUS)
    return original.where(meaningful_mask, "")


def _concat_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate multiple dataframes, filtering out None/empty ones."""
    valid_frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid_frames:
        return pd.DataFrame()
    return pd.concat(valid_frames, ignore_index=True)
