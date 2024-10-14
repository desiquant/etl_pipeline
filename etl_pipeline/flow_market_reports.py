from loguru import logger
from prefect import flow, task
from scraper.data.annual_reports import Scraper
from scraper.data.annual_reports.nse import nse_cookies

from etl_pipeline.symbols import nifty_50_symbols
from etl_pipeline.utils import sync_s3


async def nifty_50_annual_reports(source: str):
    if source == "nse":
        nse_cookies.generate()

    ar_scraper = Scraper(test_run="trial")
    await ar_scraper.scrape(nifty_50_symbols[2:4], source=source)


@flow
async def market_reports():
    # save logs to disk
    logger.add("logs/market_reports/{time:YYYY-MM-DD/HH:mm:ss}.log")

    await nifty_50_annual_reports(source="bse")
    await sync_s3(include=["annual_reports/*"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(market_reports())
