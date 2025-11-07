"""Generate a daily market report using stored price data and SMA events."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import pandas as pd
from sqlalchemy import Table, text
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
        help="Report date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "data"),
        help="Directory to write the report (default: %(default)s)",
    )
    parser.add_argument(
        "--volume-window",
        type=int,
        default=30,
        help="Rolling window (days) for average volume comparison (default: %(default)s)",
    )
    parser.add_argument(
        "--gain-threshold",
        type=float,
        default=10.0,
        help="Minimum percentage gain to list in top gainers (default: %(default)s)",
    )
    add_config_argument(parser)
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s)",
    )
    return parser.parse_args()


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def load_tables(engine: Engine) -> tuple[Table, Table, Table]:
    tables = reflect_tables(engine, "prices", "companies", "sma_events")
    return tables["prices"], tables["companies"], tables["sma_events"]


def load_price_history(
    engine: Engine,
    prices: Table,
    target_date: date,
    lookback_days: int,
) -> pd.DataFrame:
    start_date = target_date - timedelta(days=lookback_days)
    query = (
        text(
            """
            SELECT symbol, trade_date, close, volume
            FROM prices
            WHERE trade_date BETWEEN :start_date AND :target_date
            """
        )
    )
    frame = pd.read_sql(
        query,
        engine,
        params={"start_date": start_date, "target_date": target_date},
        parse_dates=["trade_date"],
    )
    if frame.empty:
        return frame
    frame["trade_date"] = frame["trade_date"].dt.date
    return frame


def load_companies(engine: Engine, companies: Table) -> pd.DataFrame:
    frame = pd.read_sql(companies.select(), engine)
    frame = frame.rename(
        columns={
            "symbol": "symbol",
            "company_name": "company_name",
            "sector": "sector",
            "industry": "industry",
        }
    )
    return frame


def load_sma_events(engine: Engine, sma_events: Table, target_date: date) -> pd.DataFrame:
    query = (
        sma_events.select()
        .where(sma_events.c.event_date == target_date)
        .where(sma_events.c.event_type.in_(["golden_cross", "death_cross"]))
    )
    frame = pd.read_sql(query, engine)
    if frame.empty:
        return frame
    frame["event_date"] = pd.to_datetime(frame["event_date"]).dt.date
    return frame


def enrich_price_metrics(
    prices_df: pd.DataFrame,
    target_date: date,
    volume_window: int,
) -> pd.DataFrame:
    if prices_df.empty:
        return prices_df

    prices_df = prices_df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    prices_df["prev_close"] = (
        prices_df.groupby("symbol")["close"].shift(1)
    )
    prices_df["pct_change"] = (
        (prices_df["close"] - prices_df["prev_close"]) / prices_df["prev_close"] * 100
    )

    prices_df["avg_volume_window"] = (
        prices_df.groupby("symbol")["volume"]
        .apply(lambda s: s.shift(1).rolling(window=volume_window, min_periods=5).mean())
        .reset_index(level=0, drop=True)
    )

    todays = prices_df[prices_df["trade_date"] == target_date].copy()
    return todays


def merge_company_details(
    todays_df: pd.DataFrame,
    companies_df: pd.DataFrame,
) -> pd.DataFrame:
    if todays_df.empty:
        return todays_df
    return todays_df.merge(
        companies_df,
        how="left",
        left_on="symbol",
        right_on="symbol",
    )


def format_report(
    report_date: date,
    top_movers: pd.DataFrame,
    sma_events: pd.DataFrame,
    sector_leaders: pd.DataFrame,
    industry_leaders: pd.DataFrame,
    volume_spikes: pd.DataFrame,
    gain_threshold: float,
) -> str:
    lines: list[str] = []
    lines.append(f"Daily Market Report - {report_date.isoformat()}")
    lines.append("=" * 60)
    lines.append("")

    lines.append(f"Stocks Up More Than {gain_threshold:.2f}%")
    lines.append("-" * 60)
    if top_movers.empty:
        lines.append("No stocks gained above the configured threshold.")
    else:
        for _, row in top_movers.iterrows():
            company = row.get("company_name") or "N/A"
            sector = row.get("sector") or "N/A"
            industry = row.get("industry") or "N/A"
            pct_change = row["pct_change"]
            close_price = row["close"]
            prev_close = row["prev_close"]
            lines.append(
                f"- {row['symbol']}: {company} | {pct_change:.2f}% "
                f"(Close: {close_price:.2f}, Prev Close: {prev_close:.2f}) "
                f"[Sector: {sector} | Industry: {industry}]"
            )
    lines.append("")

    lines.append("SMA Events")
    lines.append("-" * 60)
    if sma_events.empty:
        lines.append("No SMA events recorded for today.")
    else:
        for _, row in sma_events.sort_values(["event_type", "symbol"]).iterrows():
            short_window = row.get("short_window")
            long_window = row.get("long_window")
            close_price = row.get("close_price")
            short_sma = row.get("short_sma")
            long_sma = row.get("long_sma")
            details = []
            if close_price is not None:
                details.append(f"Close {close_price:.2f}")
            if short_sma is not None:
                details.append(f"SMA{short_window} {short_sma:.2f}")
            if long_sma is not None:
                details.append(f"SMA{long_window} {long_sma:.2f}")
            detail_text = "; ".join(details) if details else "No SMA details"
            lines.append(
                f"- {row['symbol']}: {row['event_type']} ({detail_text})"
            )
    lines.append("")

    lines.append("Sector Leaders (Top Average % Gain)")
    lines.append("-" * 60)
    if sector_leaders.empty:
        lines.append("No sector performance data available.")
    else:
        for _, row in sector_leaders.iterrows():
            lines.append(
                f"- {row['sector']}: Avg Change {row['avg_pct_change']:.2f}% "
                f"(Top: {row['top_symbol']} {row['top_pct_change']:.2f}% - {row['top_company']})"
            )
    lines.append("")

    lines.append("Industry Leaders (Top Average % Gain)")
    lines.append("-" * 60)
    if industry_leaders.empty:
        lines.append("No industry performance data available.")
    else:
        for _, row in industry_leaders.iterrows():
            lines.append(
                f"- {row['industry']}: Avg Change {row['avg_pct_change']:.2f}% "
                f"(Top: {row['top_symbol']} {row['top_pct_change']:.2f}% - {row['top_company']})"
            )
    lines.append("")

    lines.append("Unusual Volume (>= 3x rolling average)")
    lines.append("-" * 60)
    if volume_spikes.empty:
        lines.append("No volume spikes detected.")
    else:
        for _, row in volume_spikes.iterrows():
            ratio = row["volume"] / row["avg_volume_window"] if row["avg_volume_window"] else float("inf")
            lines.append(
                f"- {row['symbol']}: Volume {row['volume']:,} (~{ratio:.2f}x avg {row['avg_volume_window']:.0f}) "
                f"| Change {row['pct_change']:.2f}%"
            )

    lines.append("")
    lines.append("End of report.")

    return "\n".join(lines)


def compute_top_gainers(enriched: pd.DataFrame, threshold: float) -> pd.DataFrame:
    if enriched.empty:
        return enriched
    filtered = enriched[
        enriched["pct_change"].notna()
        & (enriched["pct_change"] >= threshold)
    ]
    return filtered.sort_values("pct_change", ascending=False)


def compute_group_leaders(
    enriched: pd.DataFrame,
    group_col: str,
) -> pd.DataFrame:
    if enriched.empty or group_col not in enriched.columns:
        return pd.DataFrame(columns=[group_col, "avg_pct_change", "top_symbol", "top_pct_change", "top_company"])

    valid = enriched[enriched["pct_change"].notna()]
    valid = valid[valid[group_col].notna()]
    if valid.empty:
        return pd.DataFrame(columns=[group_col, "avg_pct_change", "top_symbol", "top_pct_change", "top_company"])

    group_stats = (
        valid.groupby(group_col)
        .agg(
            avg_pct_change=("pct_change", "mean"),
        )
        .reset_index()
    )

    top_per_group = (
        valid.sort_values("pct_change", ascending=False)
        .drop_duplicates(subset=[group_col])
        .rename(
            columns={
                "symbol": "top_symbol",
                "pct_change": "top_pct_change",
                "company_name": "top_company",
            }
        )[[group_col, "top_symbol", "top_pct_change", "top_company"]]
    )

    merged = group_stats.merge(top_per_group, on=group_col, how="inner")
    merged = merged.sort_values("avg_pct_change", ascending=False)
    return merged


def compute_volume_spikes(
    enriched: pd.DataFrame,
    multiplier: float = 3.0,
) -> pd.DataFrame:
    if enriched.empty:
        return enriched
    filtered = enriched[
        (enriched["avg_volume_window"].notna())
        & (enriched["avg_volume_window"] > 0)
        & (enriched["volume"] >= enriched["avg_volume_window"] * multiplier)
    ].copy()
    return filtered.sort_values("volume", ascending=False)


def ensure_output_dir(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


def write_report(content: str, output_dir: Path, report_date: date) -> Path:
    filename = f"report_{report_date.strftime('%Y%m%d')}.txt"
    ensure_output_dir(output_dir)
    report_path = output_dir / filename
    report_path.write_text(content, encoding="utf-8")
    return report_path


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s %(message)s")

    today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    report_date = args.report_date or today
    config = load_database_config_from_args(args)
    engine = create_engine_from_config(config)

    try:
        prices_table, companies_table, sma_events_table = load_tables(engine)
    except Exception as exc:
        LOGGER.error("Failed to load tables: %s", exc)
        raise

    lookback_days = max(args.volume_window * 2, 90)
    prices_df = load_price_history(engine, prices_table, report_date, lookback_days)
    companies_df = load_companies(engine, companies_table)
    sma_events_df = load_sma_events(engine, sma_events_table, report_date)

    enriched_today = enrich_price_metrics(
        prices_df,
        target_date=report_date,
        volume_window=args.volume_window,
    )
    enriched_today = merge_company_details(enriched_today, companies_df)

    top_movers = compute_top_gainers(enriched_today, args.gain_threshold)
    sector_leaders = compute_group_leaders(enriched_today, "sector")
    industry_leaders = compute_group_leaders(enriched_today, "industry")
    volume_spikes = compute_volume_spikes(enriched_today)

    report_content = format_report(
        report_date=report_date,
        top_movers=top_movers,
        sma_events=sma_events_df,
        sector_leaders=sector_leaders,
        industry_leaders=industry_leaders,
        volume_spikes=volume_spikes,
        gain_threshold=args.gain_threshold,
    )

    output_dir = Path(args.output_dir)
    report_path = write_report(report_content, output_dir, report_date)
    LOGGER.info("Report written to %s", report_path)


if __name__ == "__main__":
    main()
