"""Simple script to test email notification functionality."""
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

def test_error_notification():
    """Test sending error notification email."""
    import email_service

    logger.info("="*70)
    logger.info("TESTING ERROR NOTIFICATION EMAIL")
    logger.info("="*70)

    test_errors = [
        "Test error 1: Missing column 'Date' in file test.xlsx",
        "Test error 2: Invalid employee name in row 5",
        "Test error 3: Empty timesheet for employee John Doe",
    ]

    try:
        email_service.send_error_notification(test_errors)
        logger.info("="*70)
        logger.info("✓ EMAIL TEST COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        return 0
    except Exception as exc:
        logger.error("="*70)
        logger.error("✗ EMAIL TEST FAILED")
        logger.error("="*70)
        logger.exception("Error: %s", exc)
        return 1

def test_management_report():
    """Test sending management report email."""
    import database
    import email_service

    logger.info("="*70)
    logger.info("TESTING MANAGEMENT REPORT EMAIL")
    logger.info("="*70)

    try:
        engine = database.create_db_engine()
        email_service.send_management_timesheet_report(engine, days_back=3)
        logger.info("="*70)
        logger.info("✓ MANAGEMENT REPORT TEST COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        return 0
    except Exception as exc:
        logger.error("="*70)
        logger.error("✗ MANAGEMENT REPORT TEST FAILED")
        logger.error("="*70)
        logger.exception("Error: %s", exc)
        return 1

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test email notification functionality")
    parser.add_argument(
        "--type",
        choices=["error", "report", "both"],
        default="both",
        help="Type of email to test (default: both)"
    )

    args = parser.parse_args()

    exit_code = 0

    if args.type in ["error", "both"]:
        exit_code = test_error_notification()

    if args.type in ["report", "both"] and exit_code == 0:
        exit_code = test_management_report()

    sys.exit(exit_code)
