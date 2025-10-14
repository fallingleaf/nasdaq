# Nasdaq Data Pipeline

This repository contains a collection of CLI tools that ingest market data, compute technical signals, and generate text reports against a MySQL database. Each tool shares a common YAML configuration, so you can manage credentials and connection details in one place.

## Prerequisites

- Python 3.9+
- Docker (for running MySQL via `docker-compose.yml`)
- Polygon.io API key (for price ingestion)
- Python dependencies from `requirements.txt`

## Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure MySQL**

   ```bash
   docker-compose up -d mysql
   ```

3. **Set up configuration**

   Copy `config.yaml` (or create your own) and adjust the credentials if needed. Environment variables (`MYSQL_HOST`, `MYSQL_USER`, etc.) override values in the file.

4. **Polygon API key**

   ```bash
   export POLYGON_API_KEY=your_api_key
   ```

## Common Options

All tools accept `--config` to point to an alternate YAML file:

```bash
python3 src/prices.py --config /path/to/config.yaml
```

If omitted, `config.yaml` in the repository root is used.

## Tools

### 1. Company Import (`src/import_to_mysql.py`)

Loads company metadata (symbol, name, sector, industry, market cap) from CSVs in `data/` into the `companies` table.

```bash
python3 src/import_to_mysql.py \
  --data-dir data \
  --chunk-size 500
```

Expectations:

- Input files: `*.csv` with headers mapping to `symbol`, `company`, `sector`, `industry`, `market cap`, etc.
- The script upserts rows so reruns refresh the latest values.

### 2. Price Ingestion (`src/prices.py`)

Fetches OHLC aggregates from Polygon for every company symbol and stores them in the `prices` table. The script resumes from the last stored date per symbol, or uses the provided start date.

```bash
python3 src/prices.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --lookback-days 60 \
  --sleep 0.25
```

Flags worth noting:

- `--adjusted` / `--raw` toggles adjusted pricing.
- `--timespan` and `--multiplier` control the aggregation window (default: daily candles).
- `--sleep` adds delay between API calls to avoid rate limits.

Ensure `POLYGON_API_KEY` is set before running.

### 3. SMA Events (`src/sma_events.py`)

Calculates moving-average crossovers and price-vs-SMA cross events, persisting them into `sma_events`. On each run it only evaluates new data since the latest stored event per symbol.

```bash
python3 src/sma_events.py \
  --short-window 50 \
  --long-window 200 \
  --chunk-size 500
```

Stored event types include:

- `golden_cross` and `death_cross`
- `price_cross_short_up/down`
- `price_cross_long_up/down`

The report generator (below) filters to `golden_cross` and `death_cross`.

### 4. Daily Report (`src/generate_report.py`)

Produces a text summary under `data/report_YYYYMMDD.txt` with:

- Top gainers above a configurable percentage
- Same-day `golden_cross` and `death_cross` events
- Sector and industry leaders (average % change and top performer)
- Volume spikes (≥ 3× rolling average)

```bash
python3 src/generate_report.py \
  --report-date 2024-02-15 \
  --gain-threshold 12 \
  --volume-window 30 \
  --output-dir data/reports
```

## Suggested Daily Workflow

1. Import or refresh the `companies` table (`src/import_to_mysql.py`).
2. Ingest latest prices after market close (`src/prices.py`).
3. Update SMA and price crossover events (`src/sma_events.py`).
4. Generate the daily report (`src/generate_report.py`).

Automate these steps via cron or a scheduler, passing `--config` as needed.

## Troubleshooting

- **Missing config**: ensure `config.yaml` exists or pass `--config path`.
- **Access denied**: double-check database credentials and Docker MySQL logs.
- **Polygon errors**: confirm network access and API key validity.
- **Sandbox compile failures**: when developing in restricted environments, run `python3 -m compileall` locally to validate syntax.

## Extending

The shared helpers in `src/db.py` centralize configuration and SQLAlchemy setup. Reuse them in new scripts to keep configuration consistent. Example:

```python
from db import load_database_config_from_args, create_engine_from_config

config = load_database_config_from_args(args)
engine = create_engine_from_config(config)
```

Contributions that add new analytics, improve reporting, or expand automation are welcome.
