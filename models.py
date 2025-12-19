"""Data models for timesheet system."""
from dataclasses import dataclass, field
from datetime import date
from typing import List

import pandas as pd


@dataclass
class DateWindow:
    """Represents a date range for processing."""
    start: date
    end: date
    description: str


@dataclass
class TimesheetData:
    """Container for loaded timesheet data."""
    timesheets: pd.DataFrame = field(default_factory=pd.DataFrame)
    statuses: pd.DataFrame = field(default_factory=pd.DataFrame)
    emails: pd.DataFrame = field(default_factory=pd.DataFrame)
    errors: List[str] = field(default_factory=list)
    files_processed: int = 0


@dataclass
class ProcessingStats:
    """Statistics from timesheet processing."""
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class StatusStats:
    """Statistics from status updates."""
    updated: int = 0
    inserted: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class EmailStats:
    """Statistics from email updates."""
    saved: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class DeduplicationStats:
    """Statistics from deduplication operation."""
    total_rows_before: int = 0
    total_rows_after: int = 0
    duplicates_removed: int = 0
    errors: List[str] = field(default_factory=list)
