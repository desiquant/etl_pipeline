import os
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Literal

import pandas as pd
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from etl_pipeline.utils import csv_to_parquet, sync_s3


@task
def run_spider(spiders: list[str], custom_settings: dict):
    """
    TODO:
        - Since scrapy runs crawler with twisted reactor, it was getting hard to run each individual spider as a separate task. Ideally that is how it should work. Right now, results from all the spiders are clumped and we can not see how each spider has outputted. You can view the progress for parallel spiders in: https://github.com/desiquant/news_scraper/blob/632ca518cda3c65669cb89a68cea2af701d113a4/prefect-test.py It needs more work though
    """
    from news_scraper.spiders import (
        BusinessStandardSpider,
        BusinessTodaySpider,
        CnbcTv18Spider,
        EconomicTimesSpider,
        FinancialExpressSpider,
        FirstPostSpider,
        FreePressJournalSpider,
        IndianExpressSpider,
        MoneyControlSpider,
        NDTVProfitSpider,
        News18Spider,
        OutlookIndiaSpider,
        TheHinduBusinessLineSpider,
        TheHinduSpider,
        ZeeNewsSpider,
    )

    os.environ["SCRAPY_SETTINGS_MODULE"] = "news_scraper.settings"

    log_file = Path(
        f'logs/news_scraper/{datetime.now().strftime("%Y-%m-%d/%H:%M:%S")}.log'
    )
    log_file.parent.mkdir(parents=True, exist_ok=True)  # create log dirs

    settings = get_project_settings()
    settings.update(
        {
            # "SKIP_OUTPUT_URLS": False,  # Do not skip URLs that have already been processed
            # "CLOSESPIDER_ITEMCOUNT": 10,  # Stop after scraping 10 items
            # "CONCURRENT_REQUESTS": 5,  # If default concurrent is used, it ignores itemcount limit
            # "CLOSESPIDER_TIMEOUT": 30,  # Stop after 30 seconds,
            "HTTPCACHE_ENABLED": True,
            "LOG_FILE": str(log_file),
            **custom_settings,
        }
    )

    process = CrawlerProcess(settings=settings)

    process.crawl(BusinessStandardSpider)
    # process.crawl("businessstandard")
    # for spider in spiders:
    #     process.crawl(spider)

    process.start()


@task
def convert_to_parquet():
    """
    # TODO: specify the schema for each column
    """

    # ! TODO: get paths from scrapy.cfg?
    OUTPUT_FILEPATHS = glob(f"{os.getenv('SCRAPY_OUTPUTS_DIR')}/*.csv")

    # create parquet for each spider output
    for o in OUTPUT_FILEPATHS:
        csv_to_parquet(
            input_paths=[o],
            output_path=Path("data/s3/news") / f"{Path(o).stem}.parquet",
        )

    # create consolidated parquet for all spider outputs
    csv_to_parquet(
        input_paths=OUTPUT_FILEPATHS,
        output_path="data/s3/news.parquet",
    )

    df_news = pd.read_parquet("data/s3/news.parquet", columns=["url"])

    create_markdown_artifact(
        f"# Total New Articles\n\nThe DataFrame contains a total of {df_news.shape[0]} articles.",
        description="Articles Count Artifact",
    )


@flow
async def flow_news_scraper(mode: Literal["update", "dump"] = "update"):
    if mode == "update":
        settings = {
            "SCRAPE_MODE": "update",
        }
    elif mode == "dump":
        settings = {
            "DATE_RANGE": ("2024-01-01", datetime.today()),
            "SCRAPE_MODE": "dump",
        }
    else:
        raise NotImplementedError(f"Unknown scraping mode: {mode}")

    # run spiders. saves outputs to data folder
    run_spider(
        spiders=[
            "businessstandard",
            # "businesstoday",
            # "cnbctv18",
            # "economictimes",
            # "financialexpress",
            # "firstpost",
            # "freepressjournal",
            # "indianexpress",
            # "moneycontrol",
            # "ndtvprofit",
            # "news18",
            # "outlookindia",
            # "thehindu",
            # "thehindubusinessline",
            # "zeenews",
        ],
        custom_settings=settings,
    )

    # convert outputs to parquet files
    convert_to_parquet()

    await sync_s3(include=["news/*", "news.parquet"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(flow_news_scraper())
