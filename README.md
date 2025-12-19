# Project Controlling - Timesheet Processing System

An automated timesheet processing pipeline that reads Excel files from OneDrive, processes the data, and updates a MySQL database with comprehensive error logging and email notifications.

## Project Structure

```
Project_Controlling/
├── src/
│   ├── extractors/          # Data extraction from sources
│   │   └── excel_reader.py  # Excel file reading and validation
│   ├── loaders/             # Data loading to destinations
│   │   └── database.py      # Database operations (MySQL)
│   ├── transformers/        # Data transformation and business logic
│   │   ├── data_processor.py      # Data cleaning and normalization
│   │   ├── email_service.py       # Email notifications
│   │   ├── status_service.py      # Status update operations
│   │   └── timesheet_service.py   # Timesheet processing logic
│   ├── pipelines/           # Pipeline orchestration
│   │   └── pipeline.py      # Main processing pipeline
│   └── utilities/           # Shared utilities and configuration
│       ├── config.py        # Configuration settings
│       ├── models.py        # Data models
│       └── utils.py         # Helper functions
├── main.py                  # Entry point for running the pipeline
├── test_email.py           # Email testing utility
└── IMPROVEMENTS.md         # Detailed changelog

```

## Features

- ✅ **OneDrive Integration**: Reads timesheet Excel files from OneDrive folders
- ✅ **Smart Processing**: Identifies new records vs updates, handles duplicates
- ✅ **Email Notifications**: SMTP-based error and status reporting
- ✅ **Comprehensive Logging**: Structured logging with multiple levels (DEBUG/INFO/WARNING/ERROR)
- ✅ **Database Management**: MySQL/MariaDB with connection pooling
- ✅ **Data Validation**: Column validation, date parsing, data type checking
- ✅ **Deduplication**: Automatic duplicate removal at multiple stages

## Quick Start

### Installation

1. **Clone the repository**
   ```bash
   git clone YOUR_REPO_URL
   cd Project_Controlling
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install pandas sqlalchemy openpyxl pymysql
   ```

### Configuration

Edit `src/utilities/config.py` to configure:
- Database URL
- OneDrive folder paths
- Email SMTP settings
- Notification recipients

### Usage

**Run the pipeline:**
```bash
# Process current month (default)
python main.py

# Process with DEBUG logging
python main.py --log-level DEBUG

# Process specific date range
python main.py --start-date 2025-01-01 --end-date 2025-01-31

# Skip deduplication
python main.py --no-dedup

# Custom folders
python main.py --folders "C:/path/to/timesheets"
```

**Test email notifications:**
```bash
# Test error notifications
python test_email.py --type error

# Test management reports
python test_email.py --type report

# Test both
python test_email.py
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--folders` | Custom folder paths to search | From config |
| `--current-month` | Process current month only | Auto |
| `--start-date` | Start date (YYYY-MM-DD) | None |
| `--end-date` | End date (YYYY-MM-DD) | None |
| `--no-dedup` | Skip post-processing deduplication | False |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) | INFO |

## Logging Levels

- **ERROR**: Only critical errors
- **WARNING**: Warnings and errors
- **INFO**: General progress (recommended)
- **DEBUG**: Detailed debugging information

Example output:
```
2025-12-19 09:56:40 | INFO     | pipeline | ✓ Database connection established successfully
2025-12-19 09:56:41 | INFO     | pipeline | Loading timesheet files from 1 folders
2025-12-19 09:58:42 | INFO     | pipeline | Loaded 53 files: 778 timesheet rows, 135 status rows
2025-12-19 09:59:13 | INFO     | timesheet_service | Successfully persisted timesheet changes: 581 inserted, 197 updated
```

## Architecture

### Data Flow

1. **Extract**: Read Excel files from OneDrive folders
2. **Transform**: Clean, validate, and deduplicate data
3. **Load**: Insert/update records in MySQL database
4. **Notify**: Send email notifications for errors

### Module Responsibilities

- **extractors**: Reading data from external sources (Excel files)
- **loaders**: Writing data to destinations (database)
- **transformers**: Business logic and data transformations
- **pipelines**: Orchestrating the overall workflow
- **utilities**: Configuration and shared helpers

## Email Notifications

The system sends two types of emails:

1. **Error Notifications**: Sent when processing errors occur
2. **Management Reports**: Summary of problematic timesheets

Configure recipients in `src/utilities/config.py`:
```python
NOTIFICATION_RECIPIENTS = [
    "user1@company.com",
    "user2@company.com",
]
```

## Database Schema

**Main Table**: `employees_check.unioned_table`

Key columns:
- Date
- Employee_Name
- Employee_Role
- Office_Location
- Project_Name, Project_ID
- Working_Hours
- Status
- Database, Interviews

## Development

### Running Tests
```bash
python test_email.py
```

### Adding New Features

1. **New Extractor**: Add to `src/extractors/`
2. **New Transformer**: Add to `src/transformers/`
3. **New Loader**: Add to `src/loaders/`
4. **Update Pipeline**: Modify `src/pipelines/pipeline.py`

### Code Organization

- Keep business logic in `transformers/`
- Keep I/O operations in `extractors/` and `loaders/`
- Use `utilities/config.py` for all configuration
- Define data models in `utilities/models.py`

## Troubleshooting

### Import Errors
- Ensure you're running from the project root
- Check that `src/` is in your Python path

### Database Connection Issues
- Verify `DB_URL` in config
- Check network connectivity
- Confirm MySQL credentials

### OneDrive Sync Issues
- Ensure OneDrive is fully synced
- Check folder paths in config
- Verify file permissions

## Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Commit with descriptive messages
5. Push and create a pull request

## License

Internal use only - Interconnection Consulting

## Support

For issues or questions, contact:
- labus@interconnectionconsulting.com
- strunakova@interconnectionconsulting.com
