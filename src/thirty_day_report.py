"""Generate a trailing 30-day market summary with price performance and SMA events."""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import Table, select, text
from sqlalchemy.engine import Engine

from db import (
    add_config_argument,
    create_engine_from_config,
    load_database_config_from_args,
    reflect_tables,
)

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-date",
        type=_parse_date,
        default=None,
        help="Report end date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Window length in days (default: %(default)s).",
    )
    parser.add_argument(
        "--top-stock-count",
        type=int,
        default=20,
        help="Number of symbols to list for largest gains (default: %(default)s).",
    )
    parser.add_argument(
        "--top-industry-count",
        type=int,
        default=10,
        help="Number of industries to list for largest gains (default: %(default)s).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "data"),
        help="Directory to write the report (default: %(default)s).",
    )
    add_config_argument(parser)
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s).",
    )
    return parser.parse_args()


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


@dataclass(frozen=True)
class ReportWindow:
    start_date: date
    end_date: date

    @classmethod
    def from_end_and_span(cls, end_date: date, lookback_days: int) -> "ReportWindow":
        if lookback_days < 1:
            raise ValueError("lookback_days must be at least 1.")
        start_date = end_date - timedelta(days=lookback_days - 1)
        return cls(start_date=start_date, end_date=end_date)


def load_tables(engine: Engine) -> tuple[Table, Table, Table]:
    tables = reflect_tables(engine, "prices", "companies", "sma_events")
    return tables["prices"], tables["companies"], tables["sma_events"]


def load_price_window(engine: Engine, window: ReportWindow) -> pd.DataFrame:
    sql = text(
        """
        SELECT symbol, trade_date, close
        FROM prices
        WHERE trade_date >= :start_date AND trade_date <= :end_date
        """
    )
    frame = pd.read_sql(
        sql,
        engine,
        params={"start_date": window.start_date, "end_date": window.end_date},
        parse_dates=["trade_date"],
    )
    if frame.empty:
        return frame
    frame["trade_date"] = frame["trade_date"].dt.date
    return frame


def compute_symbol_performance(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "start_date",
                "end_date",
                "start_close",
                "end_close",
                "pct_change",
            ]
        )

    prices = prices.dropna(subset=["close"])
    if prices.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "start_date",
                "end_date",
                "start_close",
                "end_close",
                "pct_change",
            ]
        )

    prices = prices.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    grouped = prices.groupby("symbol", as_index=False).agg(
        start_date=("trade_date", "first"),
        end_date=("trade_date", "last"),
        start_close=("close", "first"),
        end_close=("close", "last"),
    )
    grouped = grouped.dropna(subset=["start_close", "end_close"])
    grouped = grouped[grouped["start_close"] != 0]
    grouped["pct_change"] = (grouped["end_close"] - grouped["start_close"]) / grouped["start_close"] * 100
    return grouped.sort_values("pct_change", ascending=False).reset_index(drop=True)


def load_companies(engine: Engine, companies: Table) -> pd.DataFrame:
    frame = pd.read_sql(companies.select(), engine)
    return frame.rename(
        columns={
            "symbol": "symbol",
            "company_name": "company_name",
            "sector": "sector",
            "industry": "industry",
        }
    )


def merge_symbol_details(symbol_perf: pd.DataFrame, companies: pd.DataFrame) -> pd.DataFrame:
    if symbol_perf.empty:
        return symbol_perf
    return symbol_perf.merge(companies, on="symbol", how="left")


def compute_industry_performance(symbol_perf: pd.DataFrame, top_n: int) -> pd.DataFrame:
    if symbol_perf.empty:
        return pd.DataFrame(columns=["industry", "avg_pct_change", "symbol_count"])
    with_industry = symbol_perf.dropna(subset=["industry"])
    if with_industry.empty:
        return pd.DataFrame(columns=["industry", "avg_pct_change", "symbol_count"])
    industry = (
        with_industry.groupby("industry")
        .agg(
            avg_pct_change=("pct_change", "mean"),
            median_pct_change=("pct_change", "median"),
            symbol_count=("symbol", "count"),
        )
        .sort_values("avg_pct_change", ascending=False)
        .head(top_n)
        .reset_index()
    )
    return industry


def load_sma_events(
    engine: Engine,
    sma_events: Table,
    window: ReportWindow,
    event_types: Iterable[str],
) -> pd.DataFrame:
    stmt = (
        select(sma_events)
        .where(sma_events.c.event_date >= window.start_date)
        .where(sma_events.c.event_date <= window.end_date)
        .where(sma_events.c.event_type.in_(list(event_types)))
    )
    frame = pd.read_sql(stmt, engine)
    if frame.empty:
        return frame
    frame["event_date"] = pd.to_datetime(frame["event_date"]).dt.date
    return frame


def filter_long_window_events(events: pd.DataFrame, long_window: int) -> pd.DataFrame:
    if events.empty:
        return events
    return events[events["long_window"] == long_window].copy()


def format_report(
    window: ReportWindow,
    top_stocks: pd.DataFrame,
    golden_crosses: pd.DataFrame,
    long_crosses: pd.DataFrame,
    top_industries: pd.DataFrame,
    top_stock_count: int,
    top_industry_count: int,
) -> str:
    lines: list[str] = []
    lines.append(f"30-Day Market Report ({window.start_date.isoformat()} to {window.end_date.isoformat()})")
    lines.append("=" * 70)
    lines.append("")

    lines.append(f"Top {top_stock_count} Stocks by Percentage Gain")
    lines.append("-" * 70)
    if top_stocks.empty:
        lines.append("No price data available for the requested window.")
    else:
        for _, row in top_stocks.iterrows():
            company = row.get("company_name") or "N/A"
            pct_gain = row["pct_change"]
            start_close = row["start_close"]
            end_close = row["end_close"]
            lines.append(
                f"- {row['symbol']}: {company} | {pct_gain:.2f}% "
                f"(Start {row['start_date']}: {start_close:.2f} → {row['end_date']}: {end_close:.2f})"
            )
    lines.append("")

    lines.append("Golden Cross Events")
    lines.append("-" * 70)
    if golden_crosses.empty:
        lines.append("No golden cross events recorded during the window.")
    else:
        for _, row in golden_crosses.sort_values(["event_date", "symbol"]).iterrows():
            lines.append(
                f"- {row['event_date']}: {row['symbol']} "
                f"(short={row['short_window']} long={row['long_window']} close={row.get('close_price', 'N/A')})"
            )
    lines.append("")

    lines.append("200-Day SMA Price Cross Events")
    lines.append("-" * 70)
    if long_crosses.empty:
        lines.append("No 200-day SMA price cross events recorded during the window.")
    else:
        for _, row in long_crosses.sort_values(["event_date", "symbol"]).iterrows():
            direction = "↑" if row["event_type"].endswith("up") else "↓"
            lines.append(
                f"- {row['event_date']}: {row['symbol']} {direction} "
                f"(close={row.get('close_price', 'N/A')} short={row['short_window']}, long={row['long_window']})"
            )
    lines.append("")

    lines.append(f"Top {top_industry_count} Industries by Average % Gain")
    lines.append("-" * 70)
    if top_industries.empty:
        lines.append("No industry performance data available.")
    else:
        for _, row in top_industries.iterrows():
            lines.append(
                f"- {row['industry']}: {row['avg_pct_change']:.2f}% avg "
                f"(median {row['median_pct_change']:.2f}%, {row['symbol_count']} symbols)"
            )

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s %(message)s")

    report_date = args.report_date or date.today()
    if report_date > date.today():
        LOGGER.warning("Report date %s is in the future; using today's date.", report_date)
        report_date = date.today()

    window = ReportWindow.from_end_and_span(report_date, args.lookback_days)

    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)
    prices_table, companies_table, sma_events_table = load_tables(engine)

    prices_df = load_price_window(engine, window)
    symbol_perf = compute_symbol_performance(prices_df)
    companies_df = load_companies(engine, companies_table)
    symbol_perf = merge_symbol_details(symbol_perf, companies_df)

    top_stocks = symbol_perf.head(args.top_stock_count).copy()
    golden_cross_events = load_sma_events(engine, sma_events_table, window, ["golden_cross"])
    golden_cross_events = merge_symbol_details(golden_cross_events, companies_df)

    long_cross_events = load_sma_events(
        engine,
        sma_events_table,
        window,
        ["price_cross_long_up", "price_cross_long_down"],
    )
    long_cross_events = filter_long_window_events(long_cross_events, 200)
    long_cross_events = merge_symbol_details(long_cross_events, companies_df)

    top_industries = compute_industry_performance(symbol_perf, args.top_industry_count)

    report_text = format_report(
        window=window,
        top_stocks=top_stocks,
        golden_crosses=golden_cross_events,
        long_crosses=pd.DataFrame(), # long_cross_events
        top_industries=top_industries,
        top_stock_count=args.top_stock_count,
        top_industry_count=args.top_industry_count,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"thirty_day_report_{window.end_date.strftime('%Y%m%d')}.txt"
    output_path.write_text(report_text + "\n", encoding="utf-8")
    LOGGER.info("Report written to %s", output_path)

    print(report_text)


if __name__ == "__main__":
    main()
