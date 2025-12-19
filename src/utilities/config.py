"""Configuration constants and settings for timesheet system."""
import os
from typing import Set

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_URL = os.getenv(
    "TIMESHEET_DB_URL",
    "mysql+pymysql://admin:N6zmVKVW@ic-controlling.cluster-c0rbbiliflyo.eu-central-1.rds.amazonaws.com:9906/employees_check",
)

UNIONED_TABLE = "employees_check.unioned_table"
MAIL_TABLE = "employees_check.mail_table"

# ============================================================================
# EMAIL/MAILER CONFIGURATION
# ============================================================================

SMTP_SERVER = "pro.eu.turbo-smtp.com"
SMTP_PORT = 587
SMTP_USERNAME = "office@interconnectionconsulting.com"
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "1Supermailer1.")
EMAIL_FROM = "it-support@interconnectionconsulting.com"
EMAIL_FROM_NAME = "Support-Mail"

# Management notification recipients
NOTIFICATION_RECIPIENTS = [
    "labus@interconnectionconsulting.com",
    #"strunakova@interconnectionconsulting.com",
]

# ============================================================================
# FILE SYSTEM CONFIGURATION
# ============================================================================

EXCLUDED_FOLDERS: Set[str] = {"Template", "former_employees", "archive"}

DEFAULT_FOLDERS = [
    # OneDrive path to 2025 timesheets
    r"C:\Users\roman\OneDrive - Interconnection Consulting\Interconnection - Drive\Interconnection\Büro\Personal\Stundenaufzeichnung\2025",
    # Uncomment below for testing with local test files
    r"C:\Users\roman\OneDrive - Interconnection Consulting\Interconnection - Drive\Turkey\Time_Sheets\2025",
]

# ============================================================================
# COLUMN DEFINITIONS
# ============================================================================

COLUMN_MAP = {
    "Employee Name": "Employee_Name",
    "Employee Role": "Employee_Role",
    "Office Location": "Office_Location",
    "Project Name": "Project_Name",
    "Project ID": "Project_ID",
    "Working Hours": "Working_Hours",
    "Working Hours Converted": "Working_Hours_Converted",
    "Database": "Database",
    "Database Converted": "Database_Converted",
    "Status": "Status",
}

REQUIRED_CORE_COLUMNS = ["Date", "Employee Name", "Project ID"]

TIMESHEET_COLUMNS = [
    "Date",
    "Employee_Name",
    "Employee_Role",
    "Office_Location",
    "Project_Name",
    "Project_ID",
    "Working_Hours",
    "Status",
    "Interviews",
    "Database",
    "Database_Converted",
]

TIMESHEET_DEDUP_COLUMNS = [
    "Date",
    "Employee_Name",
    "Employee_Role",
    "Office_Location",
    "Project_Name",
    "Project_ID",
    "Working_Hours",
    "Status",
    "Interviews",
    "Database",
    "Database_Converted",
]

STATUS_COLUMNS = ["Date", "Employee_Name", "Project_ID", "Status"]
STATUS_DEDUP_COLUMNS = ["Date", "Employee_Name", "Project_ID"]

NUMERIC_COLUMNS = [
    "Working_Hours",
    "Working_Hours_Converted",
    "Interviews",
    "Database",
    "Database_Converted",
]

EMPLOYEE_EMAIL_COLUMNS = ["Employee Name", "Mail"]

SIGNATURE_EXTRA_COLUMNS = [
    "Employee_Name",
    "Employee_Role",
    "Office_Location",
    "Project_Name",
    "Project_ID",
    "Working_Hours",
    "Status",
    "Interviews",
    "Database",
    "Database_Converted",
]

TEXT_COLUMNS = [
    "Employee_Name",
    "Employee_Role",
    "Office_Location",
    "Project_Name",
    "Project_ID",
    "Status",
]

# ============================================================================
# BUSINESS RULES
# ============================================================================

NON_MEANINGFUL_STATUS: Set[str] = {
    "", "0", "n", "no", "none", "na", "n/a", "-", "--"
}

UNKNOWN_TEXT = "unknown"

# ============================================================================
# NOTIFICATION CONFIGURATION
# ============================================================================

# Issues with these status values are reported in management emails
PROBLEMATIC_STATUS_VALUES = {
    "Incomplete",
    "Pending",
    "Error",
    "Missing",
    "To Review",
}