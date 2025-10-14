"""Compute SMA crossover events (e.g. 50/200) and price crossovers relative to SMA."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from typing import Dict, Iterable, Iterator, List, Tuple

import pandas as pd
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    func,
    inspect,
    select,
    text,
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
        "--short-window",
        type=int,
        default=50,
        help="Short SMA window length (default: %(default)s)",
    )
    parser.add_argument(
        "--long-window",
        type=int,
        default=200,
        help="Long SMA window length (default: %(default)s)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Rows to upsert per batch (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s)",
    )
    return parser.parse_args()


def create_tables(engine: Engine) -> Tuple[Table, Table]:
    metadata = MetaData()
    prices = reflect_table(engine, "prices", metadata)
    sma_events = Table(
        "sma_events",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("event_date", Date, primary_key=True),
        Column("event_type", String(32), primary_key=True),
        Column("short_window", Integer, nullable=False),
        Column("long_window", Integer, nullable=False),
        Column("close_price", Float),
        Column("short_sma", Float),
        Column("long_sma", Float),
        Column("created_at", DateTime, server_default=func.now(), nullable=False),
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_unicode_ci",
    )
    metadata.create_all(engine, checkfirst=True)

    inspector = inspect(engine)
    column_names = {column["name"] for column in inspector.get_columns("sma_events")}
    if "close_price" not in column_names:
        LOGGER.info("Adding close_price column to sma_events table.")
        with engine.begin() as connection:
            connection.execute(text("ALTER TABLE sma_events ADD COLUMN close_price FLOAT"))
        sma_events = reflect_table(engine, "sma_events")

    return prices, sma_events


def fetch_symbols(engine: Engine, prices: Table) -> List[str]:
    with engine.connect() as connection:
        result = connection.execute(select(prices.c.symbol).distinct())
        symbols = [row[0] for row in result if row[0]]
    return symbols


def load_existing_events(
    engine: Engine, sma_events: Table
) -> Tuple[Dict[Tuple[str, date, str], bool], Dict[str, date]]:
    events: Dict[Tuple[str, date, str], bool] = {}
    latest_by_symbol: Dict[str, date] = {}
    with engine.connect() as connection:
        result = connection.execute(select(sma_events.c.symbol, sma_events.c.event_date, sma_events.c.event_type))
        for row in result:
            events[(row.symbol, row.event_date, row.event_type)] = True
            current_latest = latest_by_symbol.get(row.symbol)
            if current_latest is None or row.event_date > current_latest:
                latest_by_symbol[row.symbol] = row.event_date
    return events, latest_by_symbol


def fetch_price_history(
    engine: Engine,
    prices: Table,
    symbol: str,
    start_date: date | None = None,
) -> pd.DataFrame:
    with engine.connect() as connection:
        query = (
            select(prices.c.trade_date, prices.c.close)
            .where(prices.c.symbol == symbol)
            .order_by(prices.c.trade_date.asc())
        )
        if start_date:
            query = query.where(prices.c.trade_date >= start_date)
        result = connection.execute(query)
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame(columns=["trade_date", "close"])

    frame = pd.DataFrame(rows, columns=["trade_date", "close"])
    frame = frame.dropna(subset=["close"])
    frame["close"] = frame["close"].astype(float)
    return frame


def compute_sma_events(
    frame: pd.DataFrame,
    symbol: str,
    short_window: int,
    long_window: int,
) -> List[Dict[str, object]]:
    if frame.empty:
        return []

    frame = frame.sort_values("trade_date").reset_index(drop=True)
    frame["short_sma"] = frame["close"].rolling(window=short_window, min_periods=short_window).mean()
    frame["long_sma"] = frame["close"].rolling(window=long_window, min_periods=long_window).mean()

    events: List[Dict[str, object]] = []

    previous_short_diff: float | None = None
    previous_long_diff: float | None = None
    previous_sma_diff: float | None = None

    for _, row in frame.iterrows():
        close_price = float(row["close"])
        trade_date = row["trade_date"]
        short_sma = row["short_sma"]
        long_sma = row["long_sma"]

        short_valid = pd.notna(short_sma)
        long_valid = pd.notna(long_sma)

        if short_valid:
            short_diff = close_price - float(short_sma)
            if previous_short_diff is not None:
                if previous_short_diff <= 0 < short_diff:
                    events.append(
                        {
                            "symbol": symbol,
                            "event_date": trade_date,
                            "event_type": "price_cross_short_up",
                            "short_window": short_window,
                            "long_window": long_window,
                            "close_price": close_price,
                            "short_sma": float(short_sma),
                            "long_sma": float(long_sma) if long_valid else None,
                        }
                    )
                elif previous_short_diff >= 0 > short_diff:
                    events.append(
                        {
                            "symbol": symbol,
                            "event_date": trade_date,
                            "event_type": "price_cross_short_down",
                            "short_window": short_window,
                            "long_window": long_window,
                            "close_price": close_price,
                            "short_sma": float(short_sma),
                            "long_sma": float(long_sma) if long_valid else None,
                        }
                    )
            previous_short_diff = short_diff
        else:
            previous_short_diff = None

        if long_valid:
            long_diff = close_price - float(long_sma)
            if previous_long_diff is not None:
                if previous_long_diff <= 0 < long_diff:
                    events.append(
                        {
                            "symbol": symbol,
                            "event_date": trade_date,
                            "event_type": "price_cross_long_up",
                            "short_window": short_window,
                            "long_window": long_window,
                            "close_price": close_price,
                            "short_sma": float(short_sma) if short_valid else None,
                            "long_sma": float(long_sma),
                        }
                    )
                elif previous_long_diff >= 0 > long_diff:
                    events.append(
                        {
                            "symbol": symbol,
                            "event_date": trade_date,
                            "event_type": "price_cross_long_down",
                            "short_window": short_window,
                            "long_window": long_window,
                            "close_price": close_price,
                            "short_sma": float(short_sma) if short_valid else None,
                            "long_sma": float(long_sma),
                        }
                    )
            previous_long_diff = long_diff
        else:
            previous_long_diff = None

        if short_valid and long_valid:
            sma_diff = float(short_sma) - float(long_sma)
            if previous_sma_diff is not None:
                if previous_sma_diff <= 0 < sma_diff:
                    events.append(
                        {
                            "symbol": symbol,
                            "event_date": trade_date,
                            "event_type": "golden_cross",
                            "short_window": short_window,
                            "long_window": long_window,
                            "close_price": close_price,
                            "short_sma": float(short_sma),
                            "long_sma": float(long_sma),
                        }
                    )
                elif previous_sma_diff >= 0 > sma_diff:
                    events.append(
                        {
                            "symbol": symbol,
                            "event_date": trade_date,
                            "event_type": "death_cross",
                            "short_window": short_window,
                            "long_window": long_window,
                            "close_price": close_price,
                            "short_sma": float(short_sma),
                            "long_sma": float(long_sma),
                        }
                    )
            previous_sma_diff = sma_diff
        else:
            previous_sma_diff = None

    return events


def chunked(iterable: Iterable[Dict[str, object]], size: int) -> Iterator[List[Dict[str, object]]]:
    chunk: List[Dict[str, object]] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def upsert_events(engine: Engine, sma_events: Table, rows: List[Dict[str, object]], chunk_size: int = 500) -> int:
    if not rows:
        return 0

    inserted = 0
    with engine.begin() as connection:
        for batch in chunked(rows, chunk_size):
            stmt = mysql_insert(sma_events).values(batch)
            update_columns = {}
            for column in sma_events.columns:
                if column.primary_key or column.name == "created_at":
                    continue
                update_columns[column.name] = getattr(stmt.inserted, column.name)
            connection.execute(stmt.on_duplicate_key_update(**update_columns))
            inserted += len(batch)
    return inserted


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s %(message)s")

    if args.short_window <= 0 or args.long_window <= 0:
        raise ValueError("Window sizes must be positive integers.")
    if args.short_window >= args.long_window:
        raise ValueError("Short window must be smaller than long window.")

    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)
    prices_table, sma_events_table = create_tables(engine)

    symbols = fetch_symbols(engine, prices_table)
    if not symbols:
        LOGGER.warning("No symbols found in prices table.")
        return

    existing_events, latest_event_dates = load_existing_events(engine, sma_events_table)
    total_inserted = 0

    LOGGER.info(
        "Processing %d symbols for SMA and price crossover events (short=%d, long=%d)",
        len(symbols),
        args.short_window,
        args.long_window,
    )

    for symbol in symbols:
        latest_event_date = latest_event_dates.get(symbol)
        start_date_filter = None
        if latest_event_date:
            lookback_days = max(args.long_window, args.short_window) + 5
            start_date_filter = latest_event_date - timedelta(days=lookback_days)

        frame = fetch_price_history(engine, prices_table, symbol, start_date=start_date_filter)
        events = compute_sma_events(frame, symbol, args.short_window, args.long_window)

        new_events = [
            event
            for event in events
            if (event["symbol"], event["event_date"], event["event_type"]) not in existing_events
        ]
        if not new_events:
            LOGGER.debug("%s: no new events detected.", symbol)
            continue

        inserted = upsert_events(engine, sma_events_table, new_events, chunk_size=args.chunk_size)
        total_inserted += inserted

        for event in new_events:
            existing_events[(event["symbol"], event["event_date"], event["event_type"])] = True
            current_latest = latest_event_dates.get(event["symbol"])
            if current_latest is None or event["event_date"] > current_latest:
                latest_event_dates[event["symbol"]] = event["event_date"]

        LOGGER.info("%s: recorded %d new SMA-related events.", symbol, inserted)

    LOGGER.info("Completed processing. %d new events stored.", total_inserted)


if __name__ == "__main__":
    main()
