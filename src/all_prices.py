"""Fetch Polygon OHLC aggregates for all companies and store them in MySQL."""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Set

from polygon import RESTClient
from polygon.rest import models as polygon_models
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
        "--date",
        type=_parse_date,
        default=None,
        help="Date (YYYY-MM-DD). Default: today.",
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


def to_price_row(
    symbol: str,
    aggregate: polygon_models.Agg | polygon_models.GroupedDailyAgg,
    trade_date: date | None = None,
) -> Dict[str, object]:
    timestamp = getattr(aggregate, "timestamp", None)
    if trade_date is None:
        if timestamp is None:
            raise ValueError("Aggregate is missing timestamp and trade_date is not provided.")
        trade_dt = datetime.utcfromtimestamp(timestamp / 1000).date()
    else:
        trade_dt = trade_date
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


def date_range(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def fetch_grouped_price_rows(
    client: RESTClient,
    target_date: date,
    symbols: Set[str],
    adjusted: bool,
) -> List[Dict[str, object]]:

    aggregates = client.get_grouped_daily_aggs(
        date=target_date.isoformat(),
        adjusted=adjusted,
    )

    rows: List[Dict[str, object]] = []
    for aggregate in aggregates:
        symbol = getattr(aggregate, "ticker", None)
        if not symbol or symbol not in symbols:
            continue
        rows.append(to_price_row(symbol, aggregate, trade_date=target_date))
    return rows


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s %(message)s")

    if not args.polygon_key:
        raise RuntimeError("Polygon API key is required. Provide via --polygon-key or POLYGON_API_KEY.")

    _date = args.date or date.today()
    if _date > date.today():
        LOGGER.warning("Date %s is in the future; using today's date instead.", _date)
        _date = date.today()

    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)
    prices_table = create_tables(engine)
    symbols = fetch_symbols(engine)

    if not symbols:
        LOGGER.warning("No symbols found in companies table.")
        return

    LOGGER.info(
        "Fetching prices for %d symbols up to %s (%s)",
        len(symbols),
        _date,
        "adjusted" if args.adjusted else "raw",
    )

    client = RESTClient(args.polygon_key)

    symbol_set = set(symbols)
    try:
        rows = fetch_grouped_price_rows(
            client=client,
            target_date=_date,
            symbols=symbol_set,
            adjusted=args.adjusted,
        )
    except Exception as exc:
        LOGGER.exception("Failed to fetch grouped aggregates for %s: %s", _date, exc)
    inserted = upsert_prices(engine, prices_table, rows, args.chunk_size)
    LOGGER.info("%s: stored %d rows", _date, inserted)

if __name__ == "__main__":
    main()
