from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


@task
def run_spider():
    """
    TODO:
        - Since scrapy runs crawler with twisted reactor, it was getting hard to run each individual spider as a separate task. Ideally that is how it should work. Right now, results from all the spiders are clumped and we can not see how each spider has outputted. You can view the progress for parallel spiders in: https://github.com/desiquant/news_scraper/blob/632ca518cda3c65669cb89a68cea2af701d113a4/prefect-test.py It needs more work though
    """
    import os
    from datetime import datetime
    from pathlib import Path

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
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    os.environ["SCRAPY_SETTINGS_MODULE"] = "news_scraper.settings"

    log_file = Path(f"logs/{datetime.now().strftime("%Y-%m-%d/%H:%M:%S")}.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)  # create log dirs

    settings = get_project_settings()
    settings.update(
        {
            # "SKIP_OUTPUT_URLS": False,  # Do not skip URLs that have already been processed
            # "CLOSESPIDER_ITEMCOUNT": 10,  # Stop after scraping 10 items
            # "CONCURRENT_REQUESTS": 5,  # If default concurrent is used, it ignores itemcount limit
            # "CLOSESPIDER_TIMEOUT": 30,  # Stop after 30 seconds,
            # "DATE_RANGE": ("2024-01-01", datetime.today()),
            "SCRAPE_MODE": "update",
            "HTTPCACHE_ENABLED": True,  # Enable HTTP cache
            "LOG_FILE": str(log_file),  # Log file path
        }
    )

    process = CrawlerProcess(settings=settings)
    process.crawl(BusinessStandardSpider)
    process.crawl(BusinessTodaySpider)
    process.crawl(EconomicTimesSpider)
    process.crawl(FinancialExpressSpider)
    process.crawl(FirstPostSpider)
    process.crawl(FreePressJournalSpider)
    process.crawl(IndianExpressSpider)
    process.crawl(MoneyControlSpider)
    process.crawl(NDTVProfitSpider)
    process.crawl(News18Spider)
    process.crawl(OutlookIndiaSpider)
    process.crawl(TheHinduSpider)
    process.crawl(TheHinduBusinessLineSpider)
    process.crawl(ZeeNewsSpider)
    process.crawl(CnbcTv18Spider)
    process.start()


@task
def convert_to_parquet():
    """
    # TODO: specify the schema for each column
    """

    from glob import glob
    from pathlib import Path

    import pandas as pd

    # ! TODO: There is problem with relative imports. `from utils import csv_to_parquet` works while running locally but when prefect server executes a flow, it needs to be `from etl_pipeline.utils import csv_to_parquet`. Or else it fails with the following error: `ModuleNotFoundError: No module named 'utils'`
    from utils import csv_to_parquet

    # ! TODO: get paths from scrapy.cfg?
    OUTPUT_FILEPATHS = glob("data/outputs/*.csv")

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


@task(log_prints=True)
def upload_to_s3():
    # ! TODO: There is problem with relative imports. `from utils import csv_to_parquet` works while running locally but when prefect server executes a flow, it needs to be `from etl_pipeline.utils import csv_to_parquet`. Or else it fails with the following error: `ModuleNotFoundError: No module named 'utils'`
    from utils import upload_folder_to_s3

    upload_folder_to_s3(
        local_folder="data/s3",
        remote_folder="data",
    )


@flow
def scraping_flow():
    run_spider()
    convert_to_parquet()
    upload_to_s3()


if __name__ == "__main__":
    # scraping_flow()
    scraping_flow.serve(
        name="news_scraper",
        cron="0 * * * *",  # runs every one hour
    )
