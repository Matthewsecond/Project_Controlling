"""Utility functions for timesheet system."""
import calendar
from datetime import date, datetime
from typing import Optional

from models import DateWindow


def create_date_window(
    current_month_only: bool,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> DateWindow:
    """
    Create a date window based on parameters.
    
    Args:
        current_month_only: If True, use current month only
        start_date: Optional start date (ignored if current_month_only=True)
        end_date: Optional end date (ignored if current_month_only=True)
        
    Returns:
        DateWindow instance
    """
    today = datetime.now().date()
    
    if current_month_only:
        first = date(today.year, today.month, 1)
        last = date(
            today.year, 
            today.month, 
            calendar.monthrange(today.year, today.month)[1]
        )
        description = f"current month ({first.strftime('%B %Y')})"
    else:
        first = start_date or date(today.year, 1, 1)
        last = end_date or date(today.year, 12, 31)
        if last < first:
            first, last = last, first
        description = f"{first} to {last}"
    
    return DateWindow(start=first, end=last, description=description)
