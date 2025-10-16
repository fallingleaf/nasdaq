"""Load company data from CSV files into MySQL and refresh Polygon metrics."""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import pandas as pd
from polygon import RESTClient
from sqlalchemy import BigInteger, Column, MetaData, String, Table, inspect, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine

from db import (
    add_config_argument,
    create_engine_from_config,
    load_database_config_from_args,
    reflect_table
)

LOGGER = logging.getLogger(__name__)

TARGET_COLUMNS = (
    "symbol",
    "company_name",
    "sector",
    "industry",
    "market_cap",
    "weighted_shares_outstanding",
)

COLUMN_ALIASES: Dict[str, str] = {
    "symbol": "symbol",
    "ticker": "symbol",
    "ticker symbol": "symbol",
    "company": "company_name",
    "name": "company_name",
    "company name": "company_name",
    "sector": "sector",
    "industry": "industry",
    "market cap": "market_cap",
    "market capitalization": "market_cap",
    "weighted shares outstanding": "weighted_shares_outstanding",
    "weighted shares": "weighted_shares_outstanding",
}


MARKET_CAP_MULTIPLIERS = {
    "": 1,
    "K": 10**3,
    "M": 10**6,
    "B": 10**9,
    "T": 10**12,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--polygon-update",
        action="store_true",
        help="Update company information from Polygon",
    )
    parser.add_argument(
        "--data-dir",
        default=str(Path(__file__).resolve().parent.parent / "data"),
        help="Directory that holds the Excel files (default: %(default)s)",
    )
    add_config_argument(parser)
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Number of rows to upsert per batch (default: %(default)s)",
    )
    parser.add_argument(
        "--polygon-key",
        default=os.getenv("POLYGON_API_KEY"),
        help="Polygon API key (default: POLYGON_API_KEY env var)",
    )
    parser.add_argument(
        "--polygon-sleep",
        type=float,
        default=0.25,
        help="Seconds to sleep between Polygon API calls (default: %(default)s)",
    )
    return parser.parse_args()


def load_data_frames(data_dir: str) -> pd.DataFrame:
    pattern = os.path.join(data_dir, "*.csv")
    frames: List[pd.DataFrame] = []
    for path in glob.glob(pattern):
        LOGGER.info("Reading %s", path)
        df = pd.read_csv(path, dtype=str, delimiter=";")
        normalized_df = normalize_columns(df)

        if "symbol" not in normalized_df.columns:
            LOGGER.warning("Skipping %s because symbol column is missing", path)
            continue

        missing_columns = [col for col in TARGET_COLUMNS if col not in normalized_df.columns]
        if missing_columns:
            LOGGER.warning(
                "File %s is missing columns %s; filling them with null values",
                path,
                ", ".join(missing_columns),
            )

        frames.append(normalized_df.reindex(columns=TARGET_COLUMNS))

    if not frames:
        raise RuntimeError(f"No usable Excel files found in {data_dir}")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["symbol"])
    combined["symbol"] = combined["symbol"].str.strip().str.upper()
    combined["company_name"] = combined["company_name"].str.strip()
    combined["sector"] = combined["sector"].str.strip()
    combined["industry"] = combined["industry"].str.strip()

    if "market_cap" in combined.columns:
        combined["market_cap"] = combined["market_cap"].map(parse_market_cap)

    if "weighted_shares_outstanding" not in combined.columns:
        combined["weighted_shares_outstanding"] = pd.NA

    combined = combined.drop_duplicates(subset=["symbol"], keep="last")
    LOGGER.info("Loaded %d unique symbols", len(combined))
    return combined

def load_existing_companies(engine: Engine, companies: Table) -> pd.DataFrame:
    frame = pd.read_sql(companies.select(), engine)
    frame = frame.rename(
        columns={
            "symbol": "symbol",
            "company_name": "company_name",
            "sector": "sector",
            "industry": "industry",
            "market_cap": "market_cap"
        }
    )
    frame["weighted_shares_outstanding"] = 0
    return frame


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    rename_map: Dict[str, str] = {}
    for column in frame.columns:
        alias = COLUMN_ALIASES.get(column.strip().lower())
        if alias:
            rename_map[column] = alias
    normalized = frame.rename(columns=rename_map)
    return normalized


def parse_market_cap(raw_value: object) -> int | None:
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return 0

    value = str(raw_value).strip()
    if not value or value.lower() in {"n/a", "na", "none"}:
        return 0

    number_part, suffix = value[1:-1], value[-1]
    number = float(number_part.replace(",", ""))
    multiplier = MARKET_CAP_MULTIPLIERS.get(suffix.upper(), 1)
    return int(number * multiplier)


def create_table(engine: Engine) -> Table:
    metadata = MetaData()
    table = Table(
        "companies",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("company_name", String(255)),
        Column("sector", String(255)),
        Column("industry", String(255)),
        Column("market_cap", BigInteger),
        Column("weighted_shares_outstanding", BigInteger),
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_unicode_ci",
    )
    metadata.create_all(engine, checkfirst=True)
    ensure_weighted_shares_column(engine, table)
    return table


def ensure_weighted_shares_column(engine: Engine, table: Table) -> None:
    """Add weighted_shares_outstanding column if missing on existing tables."""

    inspector = inspect(engine)
    existing_columns = {column["name"] for column in inspector.get_columns(table.name)}
    if "weighted_shares_outstanding" not in existing_columns:
        LOGGER.info("Adding weighted_shares_outstanding column to %s table.", table.name)
        with engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE companies ADD COLUMN weighted_shares_outstanding BIGINT")
            )


def chunked(iterable: Iterable[Dict[str, object]], size: int) -> Iterator[List[Dict[str, object]]]:
    chunk: List[Dict[str, object]] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def upsert_dataframe(engine: Engine, table: Table, data: pd.DataFrame, chunk_size: int) -> None:
    records = data.to_dict(orient="records")
    updatable_columns = [col for col in TARGET_COLUMNS if col != "symbol"]

    with engine.begin() as connection:
        for batch in chunked(records, chunk_size):
            stmt = mysql_insert(table).values(batch)
            update_mapping = {column: stmt.inserted[column] for column in updatable_columns}
            connection.execute(stmt.on_duplicate_key_update(**update_mapping))
            LOGGER.debug("Upserted %d rows", len(batch))


def safe_to_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int,)):
        return int(value)
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(as_float):
        return None
    return int(as_float)


def enrich_with_polygon_metrics(
    data: pd.DataFrame,
    api_key: str,
    sleep: float,
) -> pd.DataFrame:
    if data.empty:
        return data

    client = RESTClient(api_key)
    symbols = sorted({symbol for symbol in data["symbol"].dropna().unique()})
    if not symbols:
        return data

    enriched = data.copy()
    for index, symbol in enumerate(symbols, start=1):
        try:
            details = client.get_ticker_details(symbol)
        except Exception as exc:
            LOGGER.warning("Polygon request failed for %s: %s", symbol, exc)
            continue
        if not details:
            LOGGER.debug("No ticker details returned for %s", symbol)
            continue
        
        LOGGER.info("Get Polygon result for ticker %s", symbol)

        market_cap = safe_to_int(getattr(details, "market_cap", None))
        weighted_shares = safe_to_int(getattr(details, "weighted_shares_outstanding", None))

        mask = enriched["symbol"] == symbol
        if market_cap is not None:
            enriched.loc[mask, "market_cap"] = market_cap
        if weighted_shares is not None:
            enriched.loc[mask, "weighted_shares_outstanding"] = weighted_shares

        if sleep:
            time.sleep(sleep)

        if index % 50 == 0:
            LOGGER.info("Refreshed Polygon metrics for %d/%d symbols.", index, len(symbols))

    return enriched


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    if not args.polygon_key:
        raise RuntimeError("Polygon API key is required. Provide via --polygon-key or POLYGON_API_KEY.")

    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)

    if args.polygon_update:
        companies = reflect_table(engine, "companies")
        dataframe = load_existing_companies(engine, companies)
    else:
        dataframe = load_data_frames(args.data_dir)
    
    companies_table = create_table(engine)
    dataframe = enrich_with_polygon_metrics(
        dataframe,
        api_key=args.polygon_key,
        sleep=max(args.polygon_sleep, 0.0),
    )
    upsert_dataframe(engine, companies_table, dataframe, args.chunk_size)
    LOGGER.info("Import complete.")


if __name__ == "__main__":
    main()
