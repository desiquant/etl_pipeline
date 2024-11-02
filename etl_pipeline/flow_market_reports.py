from loguru import logger
from prefect import flow, task
from scraper.data.announcements import Announcements_Scraper
from scraper.data.annual_reports import AnnualReports_Scraper
from scraper.data.annual_reports.nse import nse_cookies

from etl_pipeline.symbols import nifty_50_symbols
from etl_pipeline.utils import sync_s3,number_of_new_entries,update_json_with_latest_dates
from prefect.artifacts import create_table_artifact


@task
async def nifty_50_annual_reports(source: str):
    if source == "nse":
        nse_cookies.generate()

    ar_scraper = AnnualReports_Scraper(test_run="trial")
    await ar_scraper.scrape(nifty_50_symbols[2:4], source=source)


@task
async def nifty_50_announcements(source: str):
    an_scraper = Announcements_Scraper(test_run="trial")

    #load latest dates before scraping
    update_json_with_latest_dates(local_dir="./data/s3/announcements/bse",file_path="./data/scraper_metaInfo/latest_Announcementdate.json")

    await an_scraper.scrape(
        nifty_50_symbols[2:4],
        date_range=("2024-01-01", None),
        source=source,
    )

@task
async def create_announcements_artifact():
    artifact_dict = number_of_new_entries(local_dir="./data/s3/announcements/bse",file_path="./data/scraper_metaInfo/latest_Announcementdate.json")
    # Convert dictionary to a list of dictionaries for the table format
    artifact_dict = [{"Symbol": k, "New Entries": v} for k, v in artifact_dict.items()]

    # Create the artifact with the table data
    await create_table_artifact(
        key="new-entries-summary",
        table=artifact_dict,
        description="Table of new entries for each symbol"
    )


@flow
async def market_reports():
    # save logs to disk
    logger.add("logs/market_reports/{time:YYYY-MM-DD/HH:mm:ss}.log")

    # await nifty_50_annual_reports(source="bse")
    # await sync_s3(include=["annual_reports/*"])

    await nifty_50_announcements(source="bse")
    await create_announcements_artifact()
    await sync_s3(include=["announcements/*", "announcements_pdf/*"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(market_reports())
