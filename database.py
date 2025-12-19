"""Database operations for timesheet system."""
import logging
import os
import tempfile
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import config
from models import DateWindow, DeduplicationStats

logger = logging.getLogger(__name__)


def create_db_engine(db_url: Optional[str] = None) -> Engine:
    """
    Create and return a database engine.

    Args:
        db_url: Database URL (uses config default if not provided)

    Returns:
        SQLAlchemy Engine instance

    Raises:
        Exception: If connection fails
    """
    url = db_url or config.DB_URL

    # Mask password in logs
    masked_url = url
    if '@' in url and '://' in url:
        parts = url.split('://', 1)
        if len(parts) == 2:
            creds_and_host = parts[1]
            if '@' in creds_and_host:
                host_part = creds_and_host.split('@', 1)[1]
                masked_url = f"{parts[0]}://***:***@{host_part}"

    try:
        logger.debug("Creating database engine: %s", masked_url)
        engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=3600,
            future=True,
            connect_args={"local_infile": 1},
        )
        logger.debug("Testing database connection with SELECT 1...")
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.debug("✓ Database connection test successful")
        return engine
    except Exception as exc:
        logger.error("✗ Database connection failed to %s", masked_url)
        logger.error("Error: %s - %s", type(exc).__name__, exc)
        raise Exception(f"Failed to connect to database: {type(exc).__name__} - {exc}") from exc


def fetch_existing_timesheets(
    engine: Engine, 
    window: DateWindow
) -> pd.DataFrame:
    """
    Fetch existing timesheet records from database within date window.
    
    Args:
        engine: Database engine
        window: Date range to fetch
        
    Returns:
        DataFrame with existing records and computed keys
    """
    query = text(
        """
        SELECT Date, Employee_Name, Employee_Role, Office_Location, Project_Name,
               Project_ID, Working_Hours, Status, Interviews, `Database`, Database_Converted
        FROM employees_check.unioned_table
        WHERE Date >= :start AND Date <= :end
        """
    )
    try:
        df = pd.read_sql(query, engine, params={"start": window.start, "end": window.end})
    except Exception as exc:
        logger.warning("Failed to read existing records: %s", exc)
        return pd.DataFrame(columns=config.TIMESHEET_COLUMNS + ["simple_key", "signature"])

    if df.empty:
        return df

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    return df


def delete_timesheet_records(engine: Engine, keys: pd.DataFrame) -> None:
    """
    Delete timesheet records based on key values.
    
    Args:
        engine: Database engine
        keys: DataFrame with Date, Employee_Name, Project_ID columns
    """
    if keys.empty:
        return

    delete_stmt = text(
        """
        DELETE FROM employees_check.unioned_table
        WHERE DATE(Date) = :date AND Employee_Name = :employee AND Project_ID = :project
        """
    )

    params = []
    for _, row in keys.iterrows():
        date_value = row["Date"]
        if isinstance(date_value, pd.Timestamp):
            date_value = date_value.date()
        elif isinstance(date_value, datetime):
            date_value = date_value.date()
        params.append({
            "date": date_value,
            "employee": row["Employee_Name"],
            "project": row["Project_ID"],
        })

    if not params:
        return

    with engine.begin() as conn:
        conn.execute(delete_stmt, params)


def bulk_insert_timesheets(
    engine: Engine,
    df: pd.DataFrame,
    table_name: str = config.UNIONED_TABLE,
) -> int:
    """
    Ultra-fast bulk insert via LOAD DATA LOCAL INFILE.
    
    Args:
        engine: Database engine
        df: DataFrame to insert (must have correct columns)
        table_name: Full table name (schema.table)
        
    Returns:
        Number of rows inserted
    """
    if df is None or df.empty:
        logger.info("No rows to bulk load into %s", table_name)
        return 0

    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = None, table_name

    work = df.copy()
    work = work[config.TIMESHEET_COLUMNS]

    tmp_path = None
    try:
        with engine.begin() as conn:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
            ) as tmp:
                tmp_path = tmp.name
                work.to_csv(tmp, index=False, na_rep="\\N")

            fq_table = ".".join(
                _quote_identifier(x) for x in ([schema, table] if schema else [table])
            )
            cols_sql = ", ".join(_quote_identifier(c) for c in config.TIMESHEET_COLUMNS)

            load_sql = text(f"""
                LOAD DATA LOCAL INFILE :path
                INTO TABLE {fq_table}
                CHARACTER SET utf8mb4
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                ESCAPED BY '\\\\'
                LINES TERMINATED BY '\\n'
                IGNORE 1 LINES
                ({cols_sql})
            """)
            conn.execute(load_sql, {"path": tmp_path})

        logger.info("LOAD DATA inserted ~%s rows into %s", len(work), table_name)
        return len(work)

    except Exception as e:
        logger.warning("LOAD DATA LOCAL INFILE failed (%s). Falling back to to_sql.", e)
        work.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        logger.info("Fallback to to_sql inserted %s rows into %s", len(work), table_name)
        return len(work)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def update_status_records(
    engine: Engine,
    updates: pd.DataFrame,
) -> int:
    """
    Update status field for existing records.
    
    Args:
        engine: Database engine
        updates: DataFrame with Date, Employee_Name, Project_ID, Status columns
        
    Returns:
        Number of records updated
    """
    if updates.empty:
        return 0

    update_stmt = text(
        """
        UPDATE employees_check.unioned_table
        SET Status = :status
        WHERE DATE(Date) = :date AND Employee_Name = :employee AND Project_ID = :project
        """
    )
    
    params = []
    for _, row in updates.iterrows():
        params.append({
            "date": row["Date"].date() if isinstance(row["Date"], (datetime, pd.Timestamp)) else row["Date"],
            "employee": row["Employee_Name"],
            "project": row["Project_ID"],
            "status": row["Status"],
        })
    
    if not params:
        return 0

    with engine.begin() as conn:
        conn.execute(update_stmt, params)
    
    return len(params)


def fetch_and_update_emails(
    engine: Engine,
    new_emails: pd.DataFrame,
) -> int:
    """
    Update employee email table with new emails.
    
    Args:
        engine: Database engine
        new_emails: DataFrame with Employee_Name and Mail columns
        
    Returns:
        Total number of email records saved
    """
    if new_emails.empty:
        return 0

    try:
        existing = pd.read_sql(f"SELECT * FROM {config.MAIL_TABLE}", engine)
    except Exception:
        existing = pd.DataFrame(columns=["Employee_Name", "Mail"])

    combined = pd.concat([existing, new_emails], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Employee_Name"], keep="last")
    combined.to_sql(
        name=config.MAIL_TABLE.split(".")[-1],
        con=engine,
        if_exists="replace",
        index=False,
    )
    return len(combined)


def deduplicate_table_sql(
    engine: Engine,
    window: Optional[DateWindow] = None,
    table_name: str = config.UNIONED_TABLE,
) -> DeduplicationStats:
    """
    Remove duplicates from table using pure SQL (fast).
    
    Args:
        engine: Database engine
        window: Optional date range to deduplicate
        table_name: Full table name
        
    Returns:
        Deduplication statistics
    """
    stats = DeduplicationStats()
    
    try:
        logger.info("Starting SQL deduplication for %s...", table_name)

        with engine.begin() as conn:
            # Count before
            if window:
                count_query = text(f"""
                    SELECT COUNT(*) as total
                    FROM {table_name}
                    WHERE Date >= :start AND Date <= :end
                """)
                result = conn.execute(count_query, {"start": window.start, "end": window.end})
            else:
                count_query = text(f"SELECT COUNT(*) as total FROM {table_name}")
                result = conn.execute(count_query)

            stats.total_rows_before = result.fetchone()[0]
            logger.info("Total rows before deduplication: %d", stats.total_rows_before)

            if stats.total_rows_before == 0:
                logger.info("Table is empty, nothing to deduplicate")
                return stats

            # Create temp table with row numbers
            if window:
                temp_table_sql = text(f"""
                    CREATE TEMPORARY TABLE temp_dedup_keys AS
                    SELECT 
                        Date,
                        Employee_Name,
                        Project_ID,
                        ROW_NUMBER() OVER (
                            PARTITION BY DATE(Date), Employee_Name, Project_ID 
                            ORDER BY Date DESC, 
                                     COALESCE(Working_Hours, 0) DESC,
                                     COALESCE(Status, '') DESC
                        ) as rn
                    FROM {table_name}
                    WHERE Date >= :start AND Date <= :end
                """)
                conn.execute(temp_table_sql, {"start": window.start, "end": window.end})
            else:
                temp_table_sql = text(f"""
                    CREATE TEMPORARY TABLE temp_dedup_keys AS
                    SELECT 
                        Date,
                        Employee_Name,
                        Project_ID,
                        ROW_NUMBER() OVER (
                            PARTITION BY DATE(Date), Employee_Name, Project_ID 
                            ORDER BY Date DESC,
                                     COALESCE(Working_Hours, 0) DESC,
                                     COALESCE(Status, '') DESC
                        ) as rn
                    FROM {table_name}
                """)
                conn.execute(temp_table_sql)

            # Count duplicates
            count_dupes = text("SELECT COUNT(*) as dupes FROM temp_dedup_keys WHERE rn > 1")
            result = conn.execute(count_dupes)
            stats.duplicates_removed = result.fetchone()[0]

            if stats.duplicates_removed == 0:
                logger.info("No duplicates found in the table")
                stats.total_rows_after = stats.total_rows_before
                conn.execute(text("DROP TEMPORARY TABLE IF EXISTS temp_dedup_keys"))
                return stats

            logger.info("Found %d duplicate rows to remove", stats.duplicates_removed)

            # Delete duplicates
            delete_sql = text(f"""
                DELETE t
                FROM {table_name} t
                INNER JOIN temp_dedup_keys d
                    ON DATE(t.Date) = DATE(d.Date)
                    AND t.Employee_Name = d.Employee_Name
                    AND t.Project_ID = d.Project_ID
                    AND d.rn > 1
            """)
            conn.execute(delete_sql)

            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS temp_dedup_keys"))

            stats.total_rows_after = stats.total_rows_before - stats.duplicates_removed

            logger.info(
                "✓ SQL Deduplication complete: %d rows before, %d rows after, %d duplicates removed",
                stats.total_rows_before,
                stats.total_rows_after,
                stats.duplicates_removed
            )

    except Exception as exc:
        error_msg = f"Error during SQL deduplication: {exc}"
        stats.errors.append(error_msg)
        logger.exception("Failed to deduplicate table with SQL")

    return stats


def _quote_identifier(name: str) -> str:
    """Backtick-quote a SQL identifier."""
    return "`" + str(name).replace("`", "``") + "`"
