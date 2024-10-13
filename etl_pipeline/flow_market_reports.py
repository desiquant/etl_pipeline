from loguru import logger
from prefect import flow, task
from scraper.data.annual_reports.nse import AnnualReports, nse_cookies

from etl_pipeline.symbols import nifty_50_symbols
from etl_pipeline.utils import sync_s3


@task
def generate_cookies():
    """Generates nse cookies required for downloading data"""
    nse_cookies.generate()


@task
async def annual_reports():
    ar = AnnualReports(test_run="trial")
    await ar.scrape(nifty_50_symbols[2:4])


@flow
async def market_reports():
    # save logs to disk
    logger.add("logs/market_reports/{time:YYYY-MM-DD/HH:mm:ss}.log")

    generate_cookies()
    await annual_reports()
    await sync_s3(include=["annual_reports/*"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(market_reports())
