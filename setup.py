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
        "loguru",
        # NOTE: Ensure the client has SSH access to these repositories
        "news_ml @ git+ssh://git@github.com/desiquant/news_ml.git",
        "news_scraper @ git+ssh://git@github.com/desiquant/news_scraper.git",
        "scraper @ git+ssh://git@github.com/desiquant/scraper.git",
    ],
    extras_require={
        "test": [
            "pytest",
        ],
    },
    url="https://github.com/desiquant/etl_pipeline",
    python_requires=">=3.10",
)
