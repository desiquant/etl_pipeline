import pandas as pd
from prefect import flow, task


@task(log_prints=True)
def get_unlabelled_news_task(
    df_news: pd.DataFrame, df_news_l: pd.DataFrame
) -> pd.DataFrame:
    """
    Get the news articles that are unlabelled, by taking the difference between existing articles and already labelled articles

    Parameters:
        - df_news: dataframe of news articles
        - df_news_l: dataframe of news articles with labels
    """

    # get unlabelled articles
    df_news_ul = pd.merge(
        df_news, df_news_l[["url"]], on="url", how="left", indicator=True
    )
    df_news_ul = df_news_ul[df_news_ul["_merge"] == "left_only"]
    df_news_ul = df_news_ul.drop(columns=["_merge"])

    # restrict: properly formatted articles
    df_news_ul = df_news_ul[~df_news_ul["article_text"].isna()]

    print("Total unlabelled articles:", df_news_ul.shape[0])

    # restrict: articles since start of month
    df_news_ul = df_news_ul[
        df_news_ul[["date_modified", "date_published"]].max(axis=1)
        >= pd.Timestamp.today() - pd.offsets.MonthBegin(2)
    ]

    # restrict: max 1000 articles to label
    df_news_ul = df_news_ul[:1000]

    return df_news_ul


@task(log_prints=True)
async def label_news_task(df_news_ul: pd.DataFrame) -> pd.DataFrame:
    """
    Given a list of news articles, labels then

    Parameters:
        - df_news_ul - dataframe of news articles without labels

    """

    from news_ml.company import label

    df_news_l = await label(
        df=df_news_ul,
        concurrency=10,
        sample_run=True,
    )

    print("Total newly labelled articles:", df_news_l.shape[0])

    return df_news_l


NEWS_URIS = ["data/s3/news.parquet", "s3://arthanex/data/news.parquet"]
NEWS_LABELLED_URIS = [
    "data/s3/news_labelled.parquet",
    "s3://arthanex/data/news_labelled.parquet",
]


@task(log_prints=True)
def download_data():
    import os

    df = pd.read_parquet(NEWS_URIS[0 if os.path.isfile(NEWS_URIS[0]) else 1])
    # df = pd.read_parquet("../cache/news.parquet")

    # parse the date columns
    # TODO: this must be done when the parquet file is generated so that all the columns have the correct schema.
    for c in ["date_modified", "date_published"]:
        df[c] = (
            pd.to_datetime(df[c], format="mixed", errors="coerce", utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )

    df = df.sort_values(by=["date_modified", "date_published"], ascending=False)

    try:
        df_l = pd.read_parquet(
            NEWS_LABELLED_URIS[0 if os.path.isfile(NEWS_LABELLED_URIS[0]) else 1]
        )
    except FileNotFoundError:
        df_l = pd.DataFrame(columns=["url"])

    return (df, df_l)


@flow(log_prints=True)
async def label_news_flow():
    """
    News article labelling flows that does the following:
        - Downloads news articles and the labelled dataset from S3
        - Labels the unlabelled news articles
        - Save the result to S3
    """

    df, df_l = download_data()

    # latest unlabeled-only news
    df_ul = get_unlabelled_news_task(df, df_l)

    # label latest unlabelled-only news
    df_l_new = await label_news_task(df_ul)

    # merge labelled news with existing news
    df_l = pd.concat([df_l, df_l_new], ignore_index=True)

    print("Total labelled articles:", df_l.shape[0])

    # save labelled news articles to local, s3
    df_l.to_parquet(NEWS_LABELLED_URIS[0])
    df_l.to_parquet(NEWS_LABELLED_URIS[1])

    print("Uploaded labelled dataset to S3")


if __name__ == "__main__":
    # import asyncio
    # asyncio.run(label_news_flow())
    label_news_flow.serve(
        name="news-article-label",
        cron="30 * * * *",
    )
