# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based market data pipeline that:
1. Ingests stock market data from Polygon.io API
2. Stores OHLC price data and company metadata in MySQL
3. Computes technical indicators (SMA crossovers: golden cross, death cross)
4. Generates daily and 30-day market reports
5. Provides an AI-powered conversational interface for querying market data
6. Includes a web-based query interface for executing read-only SQL queries

## Tech Stack

- **Python**: 3.9+
- **Database**: MySQL 8.0 (via Docker Compose)
- **ORM**: SQLAlchemy
- **Data Processing**: pandas
- **Market Data API**: Polygon.io (using custom "massive" client library)
- **AI Agent**: OpenAI Agents SDK
- **Web Framework**: Flask (for query interface)

## Architecture

### Database Schema

**companies table:**
- `symbol` (PK): Stock ticker
- `company_name`, `sector`, `industry`
- `market_cap`, `weighted_shares_outstanding`

**prices table:**
- Composite PK: `symbol`, `trade_date`
- OHLC data: `open`, `high`, `low`, `close`
- Volume metrics: `volume`, `vwap`, `transactions`

**sma_events table:**
- Composite PK: `symbol`, `event_date`
- `event_type`: "golden_cross" or "death_cross"
- `sma_50`, `sma_200`: Simple moving average values at event time

### Data Flow

```
Polygon.io API
    ↓
import_companies.py → companies table
    ↓
get_all_prices.py / get_prices.py → prices table
    ↓
compute_sma_events.py → sma_events table
    ↓
daily_report.py / report_30days.py → Text reports (data/*.txt)
    ↓
agent.py → Conversational query interface
```

### Project Structure

- `src/db.py`: Shared database configuration and SQLAlchemy utilities
- `src/import_companies.py`: Fetch and upsert company metadata
- `src/get_prices.py`: Fetch prices for a single symbol (incremental)
- `src/get_all_prices.py`: Bulk fetch prices for all companies (parallel)
- `src/compute_sma_events.py`: Calculate SMA crossover events
- `src/daily_report.py`: Generate daily market summary
- `src/report_30days.py`: Generate 30-day market summary
- `src/agent.py`: OpenAI-based conversational agent
- `src/webapp.py`: Flask web application for read-only database queries
- `src/templates/index.html`: Web interface for query tool
- `data/`: Output directory for reports (not tracked in git)
- `config.yaml`: Configuration file for API keys, database, rate limits

## Development Setup

### Prerequisites
- Python 3.9+
- Docker and Docker Compose
- Polygon.io API key

### Installation

1. Start MySQL database:
   ```bash
   docker-compose up -d
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure `config.yaml` with your API keys:
   ```yaml
   polygon:
     api_key: "YOUR_API_KEY"
   openai:
     api_key: "YOUR_OPENAI_KEY"
   database:
     host: "localhost"
     port: 3306
     user: "nasdaq_user"
     password: "nasdaq_pass"
     database: "nasdaq"
   ```

4. Initialize database schema:
   ```bash
   python src/init_db.py
   ```

## Common Commands

### Database Management
```bash
# Start MySQL
docker-compose up -d

# Stop MySQL
docker-compose down

# Initialize/recreate database schema
python src/init_db.py
```

### Data Ingestion
```bash
# Import company metadata
python src/import_companies.py

# Fetch all historical prices (bulk, parallel)
python src/get_all_prices.py

# Fetch prices for a single symbol
python src/get_prices.py --symbol AAPL

# Fetch prices for specific date range
python src/get_prices.py --symbol AAPL --from 2024-01-01 --to 2024-12-31

# Update only companies missing data
python src/get_all_prices.py --update-only
```

### Analytics
```bash
# Compute SMA crossover events for all companies
python src/compute_sma_events.py

# Compute for single symbol
python src/compute_sma_events.py --symbol AAPL
```

### Reporting
```bash
# Generate daily market report
python src/daily_report.py

# Generate 30-day report
python src/report_30days.py
```

### AI Agent
```bash
# Start conversational agent
python src/agent.py
```

### Web Query Interface
```bash
# Start web application for read-only database queries
python src/webapp.py

# Access at http://localhost:5001
# Features:
# - Browse available tables and schemas
# - Execute SELECT queries
# - View results in formatted tables
# - Read-only validation (prevents INSERT/UPDATE/DELETE)
```

## Development Patterns

### Database Access Pattern
All scripts use the centralized database configuration from `src/db.py`:
```python
from db import load_database_config, create_engine_from_config

# Load database configuration
db_config = load_database_config()

# Create SQLAlchemy engine
engine = create_engine_from_config(db_config)

# Use engine for queries
with engine.connect() as conn:
    result = conn.execute(sqlalchemy.text("SELECT * FROM companies"))
```

### Configuration Management
- Configuration is loaded from `config.yaml`
- Environment variables override config file values
- Pattern: `os.getenv('KEY', config['section']['key'])`

### Incremental Updates (Idempotent Design)
Scripts resume from the last stored data:
- `get_prices.py`: Queries max date from prices table, fetches only newer data
- `get_all_prices.py`: Skips companies with existing data unless `--update-only` flag is used
- Database operations use `ON DUPLICATE KEY UPDATE` for upserts

### Batch Processing
Large datasets are processed in chunks:
- `get_all_prices.py`: Processes companies in batches, commits after each batch
- Rate limiting: Configurable sleep between API calls to respect Polygon.io limits

## Important Notes

### Timezone Handling
Reports use Los Angeles timezone (`America/Los_Angeles`) for date calculations and display.

### API Rate Limits
Configure `sleep_seconds` in `config.yaml` under the `polygon` section to control API request frequency and avoid rate limiting.

### Data Directory
The `data/` directory stores generated reports but is not tracked in git (see `.gitignore`).

### Resume Behavior
Most scripts are designed to be rerun safely:
- They check for existing data and only fetch missing records
- Use `--from` and `--to` flags to override and force specific date ranges

## Extending the Codebase

### Adding New Analytics
1. Query data using `src/db.py` utilities
2. Use pandas for data manipulation
3. Store results in a new table or generate reports
4. Follow the pattern in `compute_sma_events.py`

### Adding New Reports
1. Query computed analytics from database
2. Format output using the pattern in `daily_report.py`
3. Save to `data/` directory with descriptive filename

### Reusing Shared Utilities
- Database access: Import from `src/db.py`
- Configuration loading: Follow pattern in existing scripts
- Logging: Use Python's `logging` module (configured in most scripts)

## Production Deployment

The web query interface can be deployed to production using nginx and gunicorn.

### Deployment Files

- `deployment/nginx.conf`: Nginx configuration for reverse proxy
- `deployment/nasdaq-webapp.service`: Systemd service file
- `deployment/gunicorn.conf.py`: Gunicorn configuration
- `deployment/README.md`: Detailed deployment guide

### Quick Deployment Overview

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Configure systemd service**: Copy `deployment/nasdaq-webapp.service` to `/etc/systemd/system/`
3. **Configure nginx**: Copy `deployment/nginx.conf` to `/etc/nginx/sites-available/`
4. **Start services**:
   ```bash
   sudo systemctl start nasdaq-webapp
   sudo systemctl reload nginx
   ```
5. **Access**: The application will be available at `http://www.masionias.com/db/`

### Environment Variables

The Flask app supports these environment variables:
- `APPLICATION_ROOT`: URL prefix (default: `/`, set to `/db` for production)
- Standard database environment variables (see `src/db.py`)

### Production Server

- **WSGI Server**: Gunicorn (configured for 4 workers, port 8000)
- **Reverse Proxy**: Nginx (handles `/db/` location)
- **Process Manager**: systemd
- **Logs**: `/var/log/nasdaq/` (access.log, error.log)

### URL Path Handling

The web application automatically detects its base path from the browser URL:
- When deployed at `/db/`, all API calls use `/db/query`, `/db/tables`, etc.
- When deployed at root `/`, all API calls use `/query`, `/tables`, etc.
- No code changes needed - the JavaScript in `index.html` automatically detects the path

For complete deployment instructions, see `deployment/README.md`.
