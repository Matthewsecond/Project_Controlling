"""Main orchestration pipeline for timesheet processing."""
import logging
import time
from datetime import date
from pathlib import Path
from typing import List, Optional, Sequence

from src.utilities import config, utils
from src.utilities.models import DateWindow
from src.extractors import excel_reader
from src.loaders import database
from src.transformers import data_processor, email_service, status_service, timesheet_service

logger = logging.getLogger(__name__)


def create_management_mail(
    days_back: int = 3,
) -> None:
    """
    Generate and send management report of problematic timesheets.

    Creates a summary report of timesheet issues from the last N days and
    sends it to configured management recipients. If no issues found, sends
    a success notification.

    Args:
        days_back: Number of days to report on (default 3)
    """
    logger.info("=" * 70)
    logger.info("GENERATING MANAGEMENT TIMESHEET REPORT")
    logger.info("=" * 70)

    try:
        engine = database.create_db_engine()
        email_service.send_management_timesheet_report(engine, days_back=days_back)
        logger.info("Management report completed successfully")
    except Exception as exc:
        logger.exception("Failed to generate management report: %s", exc)


def process_timesheets(
    folder_paths: Sequence[str | Path],
    window: DateWindow,
) -> List[str]:
    """
    Process timesheet data from folders for a given date window.

    Args:
        folder_paths: Folders to search for Excel files
        window: Date range to process

    Returns:
        List of error messages
    """
    logger.info("Processing timesheets for %s", window.description)
    error_messages: List[str] = []

    # Connect to database
    try:
        logger.info("Connecting to database...")
        engine = database.create_db_engine()
        logger.info("✓ Database connection established successfully")
    except Exception as exc:
        error_msg = f"✗ CRITICAL: Database connection failed - {type(exc).__name__}: {exc}"
        logger.error(error_msg)
        return [error_msg]

    # Load data from Excel files
    logger.info("Loading timesheet files from %d folders", len(folder_paths))
    payload = excel_reader.load_timesheet_files(folder_paths, window)
    error_messages.extend(payload.errors)

    logger.info(
        "Loaded %d files: %d timesheet rows, %d status rows, %d email rows",
        payload.files_processed,
        len(payload.timesheets),
        len(payload.statuses),
        len(payload.emails),
    )

    if payload.timesheets.empty and payload.statuses.empty:
        warning_msg = f"⚠ WARNING: No timesheet data found for {window.description}"
        logger.warning(warning_msg)
        error_messages.append(warning_msg)
        return error_messages

    # Deduplicate loaded data
    payload.timesheets = data_processor.deduplicate_dataframe(
        payload.timesheets,
        config.TIMESHEET_DEDUP_COLUMNS,
        "Timesheet data",
    )
    payload.statuses = data_processor.deduplicate_dataframe(
        payload.statuses,
        config.STATUS_DEDUP_COLUMNS,
        "Status data",
    )

    # Fetch existing records for comparison
    logger.info("Fetching existing records from database")
    existing_records = database.fetch_existing_timesheets(engine, window)
    logger.info("Found %d existing records", len(existing_records))

    # Process timesheet changes
    timesheet_stats = timesheet_service.process_timesheet_changes(
        payload.timesheets,
        existing_records,
        engine,
    )
    error_messages.extend(timesheet_stats.errors)

    # Refresh existing records for status updates
    refreshed_records = database.fetch_existing_timesheets(engine, window)

    # Process status updates
    status_stats = status_service.process_status_updates(
        payload.statuses,
        refreshed_records,
        engine,
    )
    error_messages.extend(status_stats.errors)

    # Update employee emails
    email_stats = email_service.update_employee_emails(payload.emails, engine)
    error_messages.extend(email_stats.errors)

    # Log summary
    logger.info(
        "Processing complete - Timesheets: %d inserted, %d updated, %d skipped | "
        "Status: %d updated, %d inserted | Emails: %d saved",
        timesheet_stats.inserted,
        timesheet_stats.updated,
        timesheet_stats.skipped,
        status_stats.updated,
        status_stats.inserted,
        email_stats.saved,
    )

    return error_messages


def run_full_pipeline(
    folder_paths: Optional[Sequence[str | Path]] = None,
    current_month_only: bool = True,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    run_deduplication: bool = True,
    send_management_report: bool = False,
    report_days_back: int = 3,
) -> None:
    """
    Run the complete timesheet processing pipeline.

    Args:
        folder_paths: Folders to search (uses config defaults if not provided)
        current_month_only: If True, process current month only
        start_date: Start date for processing (ignored if current_month_only=True)
        end_date: End date for processing (ignored if current_month_only=True)
        run_deduplication: If True, run post-processing deduplication
        send_management_report: If True, generate and send management report
        report_days_back: Number of days to include in management report (default 3)
    """
    folders = list(folder_paths) if folder_paths else config.DEFAULT_FOLDERS
    window = utils.create_date_window(current_month_only, start_date, end_date)

    logger.info("=" * 70)
    logger.info("STARTING TIMESHEET PROCESSING PIPELINE")
    logger.info("Date range: %s", window.description)
    logger.info("Folders: %d", len(folders))
    logger.info("=" * 70)

    start_time = time.time()

    # Process timesheets
    errors = process_timesheets(folders, window)

    # Send error notifications
    if errors:
        logger.warning("="*70)
        logger.warning("⚠ PROCESSING COMPLETED WITH %d ERROR(S):", len(errors))
        for i, error in enumerate(errors[:10], 1):  # Show first 10 errors in log
            logger.warning("  %d. %s", i, error)
        if len(errors) > 10:
            logger.warning("  ... and %d more errors (see full list in email)", len(errors) - 10)
        logger.warning("="*70)
        logger.info("Attempting to send error notification email...")
        try:
            email_service.send_error_notification(errors)
            logger.info("✓ Error notification email sent successfully")
        except Exception as exc:
            logger.error("✗ Failed to send error notification: %s", exc)
    else:
        logger.info("✓ Processing completed successfully with no errors")

    # Run deduplication if requested
    if run_deduplication:
        logger.info("=" * 70)
        logger.info("RUNNING POST-PROCESS DEDUPLICATION")
        logger.info("=" * 70)

        try:
            logger.info("Connecting to database for deduplication...")
            engine = database.create_db_engine()
            logger.info("Running SQL deduplication on table: %s", config.UNIONED_TABLE)
            dedup_stats = database.deduplicate_table_sql(
                engine,
                window=window,
                table_name=config.UNIONED_TABLE,
            )

            if dedup_stats.errors:
                logger.error("✗ Deduplication completed with %d error(s):", len(dedup_stats.errors))
                for error in dedup_stats.errors:
                    logger.error("  - %s", error)
            else:
                logger.info(
                    "✓ Deduplication successful: %d duplicates removed (%d -> %d rows)",
                    dedup_stats.duplicates_removed,
                    dedup_stats.total_rows_before,
                    dedup_stats.total_rows_after,
                )
        except Exception as exc:
            logger.error("✗ DEDUPLICATION FAILED")
            logger.exception("Error details: %s", exc)

    # Generate management report if requested
    if send_management_report:
        create_management_mail(days_back=report_days_back)

    elapsed_time = time.time() - start_time
    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE - Total time: %.2f seconds", elapsed_time)
    logger.info("=" * 70)