import os
import asyncio

from agents import Agent, function_tool, Runner, WebSearchTool
from sqlalchemy import text
from db import (
    create_engine_from_config,
    load_database_config_from_args,
)

BASE_DIR = os.path.join(os.path.dirname(__file__), "../data")
MODEL = "gpt-5-mini"


DB_INSTRUCTIONS = """
Handle company, stock price and SMA event information.
Form SQL query and read data from stock database.

There are 3 SQL tables:
    - companies: contains company sticker information.
    Table has these columns: symbol, company_name, sector, industry, market_cap

    - prices: store all sticker prices.
    Table has these columns: symbol, trade_date, open, close, low, high, volume, vwap, transactions

    - sma_events: store SMA events such as "golden_cross" and "death_cross".
    Table has these columns: symbol, event_date, event_type, short_window, long_window, short_sma, long_sma
"""

REPORT_INSTRUCTIONS = """
Read stock daily report stored in text file.
Each daily report contains:
- Stocks that up more than 10%.
- SMA Events
- Section leaders
- Industry leaders
- Stock traded with high volume.
"""

@function_tool
def query_stock_data(sql: str) -> list[dict]:
    """Returns stock prices and events using SQL query.
    """
    config = load_database_config_from_args(None)
    engine = create_engine_from_config(config)
    data = []
    with engine.connect() as connection:
        result = connection.execute(text(sql))
        for row in result.mappings():
            data.append(row)
        return data


@function_tool
def read_report_date(report_date: str) -> str:
    """Read stock report for date: YYYYmmdd.
    """
    file_path = os.path.join(BASE_DIR, f"report_{report_date}.txt")
    if not os.path.exists(file_path):
        return "No report found!"
    return open(file_path, "+rt").read()


databse_agent = Agent(
    name="Stock Database Agent",
    instructions=DB_INSTRUCTIONS,
    tools = [query_stock_data],
)

report_agent = Agent(
    name="Stock Report Agent",
    instructions=REPORT_INSTRUCTIONS,
    tools=[read_report_date],
)

agent = Agent(
    name="Stock Agent",
    instructions=(
        "Answer user's question about stock market"
        "If asking about stock report, hand off to report agent."
        "If no report found or any other stock information, hand off to database agent."
    ),
    model=MODEL,
    handoffs=[databse_agent, report_agent]
)

async def main():
    result = await Runner.run(agent, "Find SMA events for NBIS stock")
    print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())