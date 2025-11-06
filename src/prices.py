"""Fetch Polygon OHLC aggregates for all companies and store them in MySQL."""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, Iterator, List

from massive import RESTClient
from massive.rest import models as massive_models
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    Float,
    MetaData,
    String,
    Table,
    func,
    select,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine

from db import (
    add_config_argument,
    create_engine_from_config,
    load_database_config_from_args,
    reflect_table,
)

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_config_argument(parser)
    parser.add_argument(
        "--polygon-key",
        default=os.getenv("POLYGON_API_KEY"),
        help="Polygon API key (default: POLYGON_API_KEY env var)",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). If omitted, resume from last stored date or lookback window.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--multiplier",
        type=int,
        default=1,
        help="Aggregation multiplier (default: %(default)s)",
    )
    parser.add_argument(
        "--timespan",
        default="day",
        help="Aggregation timespan (default: %(default)s)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum results per request (default: %(default)s)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="When no start date and no existing data, number of days to fetch ending today (default: %(default)s)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Rows to upsert per batch (default: %(default)s)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.25,
        help="Seconds to sleep between API calls (default: %(default)s)",
    )
    parser.add_argument(
        "--adjusted",
        dest="adjusted",
        action="store_true",
        default=True,
        help="Use adjusted prices (default)",
    )
    parser.add_argument(
        "--raw",
        dest="adjusted",
        action="store_false",
        help="Use raw (unadjusted) prices",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s)",
    )
    return parser.parse_args()


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def create_tables(engine: Engine) -> Table:
    metadata = MetaData()
    prices = Table(
        "prices",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("trade_date", Date, primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", BigInteger),
        Column("vwap", Float),
        Column("transactions", BigInteger),
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_unicode_ci",
    )

    metadata.create_all(engine, checkfirst=True)
    return prices


def fetch_symbols(engine: Engine) -> List[str]:
    companies = reflect_table(engine, "companies")
    with engine.connect() as connection:
        result = connection.execute(select(companies.c.symbol))
        symbols = [row[0] for row in result if row[0]]
    return symbols


def fetch_latest_trade_dates(engine: Engine, prices: Table) -> Dict[str, date]:
    latest: Dict[str, date] = {}
    with engine.connect() as connection:
        result = connection.execute(
            select(prices.c.symbol, func.max(prices.c.trade_date).label("latest_date")).group_by(prices.c.symbol)
        )
        for row in result:
            latest[row.symbol] = row.latest_date
    return latest


def chunked(iterable: Iterable[Dict[str, object]], size: int) -> Iterator[List[Dict[str, object]]]:
    chunk: List[Dict[str, object]] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def upsert_prices(engine: Engine, table: Table, rows: List[Dict[str, object]], chunk_size: int) -> int:
    if not rows:
        return 0

    inserted = 0
    with engine.begin() as connection:
        for batch in chunked(rows, chunk_size):
            stmt = mysql_insert(table).values(batch)
            update_columns = {
                column.name: getattr(stmt.inserted, column.name)
                for column in table.columns
                if not column.primary_key
            }
            connection.execute(stmt.on_duplicate_key_update(**update_columns))
            inserted += len(batch)
    return inserted


def to_price_row(symbol: str, aggregate: massive_models.Agg) -> Dict[str, object]:
    trade_dt = datetime.utcfromtimestamp(aggregate.timestamp / 1000).date()
    return {
        "symbol": symbol,
        "trade_date": trade_dt,
        "open": aggregate.open,
        "high": aggregate.high,
        "low": aggregate.low,
        "close": aggregate.close,
        "volume": aggregate.volume,
        "vwap": getattr(aggregate, "vwap", None),
        "transactions": getattr(aggregate, "transactions", None),
    }


def fetch_price_rows(
    client: RESTClient,
    symbol: str,
    multiplier: int,
    timespan: str,
    start_date: date,
    end_date: date,
    limit: int,
    adjusted: bool,
) -> List[Dict[str, object]]:
    aggregates: List[Dict[str, object]] = []
    for agg in client.list_aggs(
        ticker=symbol,
        multiplier=multiplier,
        timespan=timespan,
        from_=start_date.isoformat(),
        to=end_date.isoformat(),
        adjusted=adjusted,
        sort="asc",
        limit=limit,
    ):
        aggregates.append(to_price_row(symbol, agg))
    return aggregates


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s %(message)s")

    if not args.polygon_key:
        raise RuntimeError("Polygon API key is required. Provide via --polygon-key or POLYGON_API_KEY.")

    end_date = args.end_date or date.today()
    if end_date > date.today():
        LOGGER.warning("End date %s is in the future; using today's date instead.", end_date)
        end_date = date.today()

    if args.start_date and args.start_date > end_date:
        raise ValueError("Start date cannot be after end date.")

    lookback_days = max(args.lookback_days, 1)

    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)
    prices_table = create_tables(engine)
    symbols = fetch_symbols(engine)

    if not symbols:
        LOGGER.warning("No symbols found in companies table.")
        return

    latest_trade_dates = fetch_latest_trade_dates(engine, prices_table)

    LOGGER.info(
        "Fetching prices for %d symbols up to %s (%s)",
        len(symbols),
        end_date,
        "adjusted" if args.adjusted else "raw",
    )
    if args.start_date:
        LOGGER.info("Using explicit start date %s for all symbols.", args.start_date)

    client = RESTClient(args.polygon_key)

    total_rows = 0
    for symbol in symbols:
        last_trade_date = latest_trade_dates.get(symbol)
        if last_trade_date:
            start_date = last_trade_date + timedelta(days=1)
        elif args.start_date:
            start_date = args.start_date
        else:
            start_date = end_date - timedelta(days=lookback_days - 1)

        if start_date > end_date:
            LOGGER.debug("%s: up to date (latest %s)", symbol, last_trade_date)
            continue

        try:
            rows = fetch_price_rows(
                client=client,
                symbol=symbol,
                multiplier=args.multiplier,
                timespan=args.timespan,
                start_date=start_date,
                end_date=end_date,
                limit=args.limit,
                adjusted=args.adjusted,
            )
        except Exception as exc:
            LOGGER.exception("Failed to fetch data for %s: %s", symbol, exc)
            continue

        inserted = upsert_prices(engine, prices_table, rows, args.chunk_size)
        total_rows += inserted
        LOGGER.info("%s: stored %d rows (from %s to %s)", symbol, inserted, start_date, end_date)

        if args.sleep:
            time.sleep(args.sleep)

    LOGGER.info("Import complete. Stored %d rows.", total_rows)


if __name__ == "__main__":
    main()
