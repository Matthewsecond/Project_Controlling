"""Email and notification services."""
import logging
from datetime import datetime
from typing import List, Sequence

import pandas as pd
from sqlalchemy.engine import Engine

from src.utilities import config
from src.utilities.models import EmailStats
from src.transformers import data_processor
from src.loaders import database

logger = logging.getLogger(__name__)


def send_management_timesheet_report(
    engine: Engine,
    days_back: int = 3,
) -> None:
    """
    Send management report of problematic timesheets from the last N days.

    Identifies employees with incomplete or erroneous timesheet entries and
    sends a summary to management recipients.

    Args:
        engine: Database engine
        days_back: Number of days to report on (default 3)
    """
    if not config.NOTIFICATION_RECIPIENTS:
        logger.info("No notification recipients configured; skipping management report")
        return

    # Try to import Mailer, fall back to local send function
    try:
        from Mailer import send_message_to_person
        use_mailer = True
    except ImportError:
        logger.warning("Mailer module not available; using local SMTP configuration")
        use_mailer = False
        send_message_to_person = _send_email_via_smtp

    # Fetch problematic timesheets
    problematic_df = database.fetch_problematic_timesheets(engine, days_back=days_back)

    if problematic_df.empty:
        logger.info("No problematic timesheets found in the last %d days", days_back)
        # Send success notification
        current_date = datetime.now().strftime("%B %d, %Y")
        success_message = f"TIMESHEET STATUS REPORT - {current_date}\n\nNo issues found in timesheets for the last {days_back} days. All timesheets are complete and properly filled out."

        for recipient in config.NOTIFICATION_RECIPIENTS:
            try:
                send_message_to_person(recipient, success_message)
                logger.info("Success notification sent to %s", recipient)
            except Exception as exc:
                logger.error("Failed to send success notification to %s: %s", recipient, exc)

        return

    # Group by employee for summary
    employee_issues = problematic_df.groupby("Employee_Name").agg({
        "Date": lambda x: x.nunique(),
        "Status": lambda x: (x.isna().sum() + (x == "").sum()),
        "Working_Hours": lambda x: (x.isna().sum() + (x == 0).sum()),
    }).rename(columns={
        "Date": "issue_days",
        "Status": "missing_status_count",
        "Working_Hours": "zero_hours_count"
    })

    # Build email body
    current_date = datetime.now().strftime("%B %d, %Y")
    body_lines = [
        f"TIMESHEET ISSUES REPORT - Last {days_back} Days ({current_date})",
        f"Total problematic records: {len(problematic_df)}",
        f"Affected employees: {len(employee_issues)}",
        "",
    ]

    # Add employee summaries
    for emp_name, row in employee_issues.iterrows():
        body_lines.append(
            f"{emp_name}: {int(row['issue_days'])} days with issues "
            f"({int(row['missing_status_count'])} missing status, {int(row['zero_hours_count'])} zero hours)"
        )

    body_lines.append("")

    # Add detailed records (limit to 30 most recent)
    for idx, row in problematic_df.head(30).iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d") if pd.notna(row["Date"]) else "N/A"
        hours = row["Working_Hours"] if pd.notna(row["Working_Hours"]) else "0"
        status = row["Status"] if pd.notna(row["Status"]) and row["Status"] else "NO STATUS"
        project = row["Project_Name"] if pd.notna(row["Project_Name"]) else row["Project_ID"]

        body_lines.append(
            f"{date_str} | {row['Employee_Name']} | {project} | Hours: {hours} | Status: {status}"
        )

    if len(problematic_df) > 30:
        body_lines.append(f"... and {len(problematic_df) - 30} more records not shown")

    body = "\n".join(body_lines)

    # Send to recipients
    for recipient in config.NOTIFICATION_RECIPIENTS:
        try:
            send_message_to_person(recipient, body)
            logger.info("Management report sent to %s", recipient)
        except Exception as exc:
            logger.error("Failed to send management report to %s: %s", recipient, exc)

    logger.info("Management report sent to %d recipients", len(config.NOTIFICATION_RECIPIENTS))


def _send_email_via_smtp(recipient: str, message_text: str) -> None:
    """
    Send email via SMTP when Mailer module is not available.

    Args:
        recipient: Email recipient address
        message_text: Email body text

    Raises:
        Exception: If email sending fails
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.header import Header
    from email.utils import formataddr

    server = None
    try:
        logger.debug("Creating email message for %s", recipient)
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "IC-ControllingTool - Timesheet Report"
        msg['From'] = formataddr((str(Header(config.EMAIL_FROM_NAME, 'utf-8')), config.EMAIL_FROM))
        msg['To'] = recipient

        part1 = MIMEText(message_text, 'plain', 'utf-8')
        msg.attach(part1)

        logger.debug("Connecting to SMTP server %s:%s", config.SMTP_SERVER, config.SMTP_PORT)
        server = smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT, timeout=30)
        server.set_debuglevel(0)  # Set to 1 for detailed SMTP debugging

        logger.debug("Starting TLS encryption")
        server.starttls()

        logger.debug("Logging in as %s", config.SMTP_USERNAME)
        server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)

        logger.debug("Sending email to %s", recipient)
        server.sendmail(config.EMAIL_FROM, recipient, msg.as_string())

        logger.debug("Email sent successfully, closing connection")
    except smtplib.SMTPAuthenticationError as exc:
        raise Exception(f"SMTP Authentication failed: {exc}. Check SMTP_USERNAME and SMTP_PASSWORD in config.") from exc
    except smtplib.SMTPException as exc:
        raise Exception(f"SMTP error: {exc}") from exc
    except Exception as exc:
        raise Exception(f"Email sending failed: {type(exc).__name__} - {exc}") from exc
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass  # Ignore errors when closing connection


def update_employee_emails(
    email_df: pd.DataFrame,
    engine: Engine,
) -> EmailStats:
    """
    Update employee email table in database.

    Args:
        email_df: DataFrame with employee names and emails
        engine: Database engine

    Returns:
        Email update statistics
    """
    stats = EmailStats()

    if email_df.empty:
        return stats

    try:
        # Prepare email data
        working = data_processor.prepare_emails_for_update(email_df)

        if working.empty:
            return stats

        # Update database
        num_saved = database.fetch_and_update_emails(engine, working)
        stats.saved = num_saved
        logger.info("Updated employee email table: %d records", stats.saved)

    except Exception as exc:
        stats.errors.append(f"Error updating employee email table: {exc}")
        logger.exception("Failed to update mail table")

    return stats


def send_error_notification(error_messages: Sequence[str]) -> None:
    """
    Send notification email with error messages.

    Args:
        error_messages: List of error messages to send
    """
    if not error_messages:
        logger.info("No errors to report; skipping notification email.")
        return

    if not config.NOTIFICATION_RECIPIENTS:
        logger.warning("No notification recipients configured; skipping error notification email.")
        return

    # Try to import Mailer, fall back to local send function
    try:
        from Mailer import send_message_to_person
        use_mailer = True
        logger.debug("Using Mailer module for sending emails")
    except ImportError:
        logger.info("Mailer module not available; using built-in SMTP configuration")
        use_mailer = False
        send_message_to_person = _send_email_via_smtp

    # Filter and clean error messages
    cleaned_messages = []
    for message in error_messages:
        if not message:
            continue
        lowered = message.lower()
        if any(
            keyword in lowered
            for keyword in ["invalid", "missing", "column", "cell", "problem", "empty", "error", "critical", "warning", "failed"]
        ):
            cleaned_messages.append(message.strip())

    if not cleaned_messages:
        logger.info("No actionable file errors to email about.")
        return

    # Limit to top 20 errors
    cleaned_messages = cleaned_messages[:20]

    # Build email body
    current_month_year = datetime.now().strftime("%B %Y")
    body_lines = [
        f"ERRORS IN TIMESHEETS FOR {current_month_year}:",
        f"Total errors: {len(error_messages)}",
        "",
    ]
    body_lines.extend(cleaned_messages)

    if len(error_messages) > len(cleaned_messages):
        body_lines.append("")
        body_lines.append(
            f"... and {len(error_messages) - len(cleaned_messages)} more issues not shown."
        )

    body = "\n".join(body_lines)

    # Send to recipients
    success_count = 0
    for recipient in config.NOTIFICATION_RECIPIENTS:
        try:
            logger.info("Sending error notification to %s...", recipient)
            send_message_to_person(recipient, body)
            logger.info("✓ Notification email sent successfully to %s", recipient)
            success_count += 1
        except Exception as exc:
            logger.error("✗ Failed to send email to %s: %s - %s", recipient, type(exc).__name__, exc)

    if success_count > 0:
        logger.info("✓ Notification emails sent to %d/%d recipients", success_count, len(config.NOTIFICATION_RECIPIENTS))
    else:
        logger.error("✗ Failed to send notification emails to all recipients")