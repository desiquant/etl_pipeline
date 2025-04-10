# ! TODO: There is problem with relative imports. Need to use `etl_pipeline.news_labelling_flow` instead for the flow to work
from news_labelling_flow import label_news_flow
from prefect import serve

from etl_pipeline.flow_news_scraper import scraping_flow

if __name__ == "__main__":
    serve(
        scraping_flow.to_deployment(name="news_scraper", cron="0 * * * *"),
        label_news_flow.to_deployment(name="news-article-label", cron="30 * * * *"),
    )
