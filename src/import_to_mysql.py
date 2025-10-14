"""Load company data from Excel files into a MySQL table."""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

import pandas as pd
from sqlalchemy import BigInteger, Column, MetaData, String, Table
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine

from db import (
    add_config_argument,
    create_engine_from_config,
    load_database_config_from_args,
)

LOGGER = logging.getLogger(__name__)

TARGET_COLUMNS = ("symbol", "company_name", "sector", "industry", "market_cap")

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

    combined = combined.drop_duplicates(subset=["symbol"], keep="last")
    LOGGER.info("Loaded %d unique symbols", len(combined))
    return combined


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
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_unicode_ci",
    )
    metadata.create_all(engine, checkfirst=True)
    return table


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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)
    companies_table = create_table(engine)

    dataframe = load_data_frames(args.data_dir)
    upsert_dataframe(engine, companies_table, dataframe, args.chunk_size)
    LOGGER.info("Import complete.")


if __name__ == "__main__":
    main()
