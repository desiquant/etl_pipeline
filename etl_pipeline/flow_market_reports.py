from loguru import logger
from prefect import flow, task
from scraper.data.announcements import Announcements_Scraper
from scraper.data.annual_reports import AnnualReports_Scraper
from scraper.data.annual_reports.nse import nse_cookies

from etl_pipeline.symbols import nifty_50_symbols
from etl_pipeline.utils import sync_s3


@task
async def nifty_50_annual_reports(source: str):
    if source == "nse":
        nse_cookies.generate()

    ar_scraper = AnnualReports_Scraper(test_run="trial")
    await ar_scraper.scrape(nifty_50_symbols[2:4], source=source)


@task
async def nifty_50_announcements(source: str):
    an_scraper = Announcements_Scraper(test_run="trial")
    await an_scraper.scrape(
        nifty_50_symbols[2:4],
        date_range=("2024-01-01", None),
        source=source,
    )


@flow
async def market_reports():
    # save logs to disk
    logger.add("logs/market_reports/{time:YYYY-MM-DD/HH:mm:ss}.log")

    # await nifty_50_annual_reports(source="bse")
    # await sync_s3(include=["annual_reports/*"])

    await nifty_50_announcements(source="bse")
    await sync_s3(include=["announcements/*", "announcements_pdf/*"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(market_reports())
