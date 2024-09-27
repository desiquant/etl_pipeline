from setuptools import find_packages, setup

setup(
    name="etl_pipeline",
    description="ETL Pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "scrapy",
        "prefect",
        "python-dotenv",
        "boto3",
        "pandas",
        "pyarrow",
        # "news_scraper @ file:///home/skd/Projects/desiquant-news_scraper",
        "news_scraper @ git+https://github.com/desiquant/news_scraper.git@master",
    ],
    extras_require={
        "test": [
            "pytest",
        ],
    },
    url="https://github.com/desiquant/etl_pipeline",
    python_requires=">=3.10",
)
