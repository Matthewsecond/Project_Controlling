"""Main entry point for timesheet processing system."""
import argparse
import logging
from datetime import date

import pipeline

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Process employee timesheet data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process current month only (default)
  python main.py

  # Process specific date range
  python main.py --start-date 2025-01-01 --end-date 2025-02-28

  # Process custom folders
  python main.py --folders "C:/timesheets/2025" "D:/backup/timesheets"

  # Skip deduplication
  python main.py --no-dedup

  # Process specific date range without deduplication
  python main.py --start-date 2025-01-01 --end-date 2025-01-31 --no-dedup
        """,
    )

    parser.add_argument(
        "--folders",
        nargs="+",
        help="Folder paths to search for timesheet files (uses defaults from config if not provided)",
    )

    parser.add_argument(
        "--current-month",
        action="store_true",
        help="Process current month only (default behavior)",
    )

    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date for processing (YYYY-MM-DD format)",
    )

    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date for processing (YYYY-MM-DD format)",
    )

    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Skip post-processing deduplication",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Configure logging with detailed format
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Determine if we're processing current month only
    current_month_only = args.current_month or (args.start_date is None and args.end_date is None)

    # Log startup info
    logger.info("="*70)
    logger.info("TIMESHEET PROCESSING SYSTEM STARTING")
    logger.info("Log level: %s", args.log_level)
    if args.folders:
        logger.info("Custom folders: %s", args.folders)
    logger.info("="*70)

    # Run the pipeline
    try:
        pipeline.run_full_pipeline(
            folder_paths=args.folders,
            current_month_only=current_month_only,
            start_date=args.start_date,
            end_date=args.end_date,
            run_deduplication=not args.no_dedup,
        )
        logger.info("="*70)
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        return 0
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    except Exception as exc:
        logger.error("="*70)
        logger.error("PIPELINE EXECUTION FAILED")
        logger.error("="*70)
        logger.exception("Fatal error: %s", exc)
        logger.error("="*70)
        return 1


if __name__ == "__main__":
    exit(main())
