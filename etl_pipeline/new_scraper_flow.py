from prefect import flow, task


@task
def run_spider():
    """
    TODO:
        - Since scrapy runs crawler with twisted reactor, it was getting hard to run each individual spider as a separate task. Ideally that is how it should work. Right now, results from all the spiders are clumped and we can not see how each spider has outputted. You can view the progress for parallel spiders in: https://github.com/desiquant/news_scraper/blob/632ca518cda3c65669cb89a68cea2af701d113a4/prefect-test.py It needs more work though
    """
    import os
    from datetime import datetime

    from news_scraper.spiders import (
        BusinessStandardSpider,
        BusinessTodaySpider,
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

    settings = get_project_settings()
    settings.update(
        {
            # "SKIP_OUTPUT_URLS": False,  # Do not skip URLs that have already been processed
            # "CLOSESPIDER_ITEMCOUNT": 10,  # Stop after scraping 10 items
            # "CONCURRENT_REQUESTS": 5,  # If default concurrent is used, it ignores itemcount limit
            # "CLOSESPIDER_TIMEOUT": 30,  # Stop after 30 seconds,
            "DATE_RANGE": ("2024-01-01", datetime.today()),
            "SCRAPE_MODE": "dump",
            "HTTPCACHE_ENABLED": True,  # Enable HTTP cache
            "LOG_FILE": "scrapy.log",  # Log file path
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
    process.start()


@task
def convert_to_parquet():
    from glob import glob
    from pathlib import Path

    from utils import jl_to_parquet

    OUTPUT_FILEPATHS = glob("data/outputs/*.jl")

    # create parquet for each spider output
    for o in OUTPUT_FILEPATHS:
        jl_to_parquet(
            input_paths=[o],
            output_path=Path("data/s3/news") / f"{Path(o).stem}.parquet",
        )

    # create consolidated parquet for all spider outputs
    jl_to_parquet(
        input_paths=OUTPUT_FILEPATHS,
        output_path="data/s3/news.parquet",
    )


@task(log_prints=True)
def upload_to_s3():
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
    scraping_flow.serve(name="news_scraper")
