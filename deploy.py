from pathlib import Path

from prefect import Flow

if __name__ == "__main__":
    Flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="etl_pipeline/flow_market_reports.py:market_reports",
    ).deploy(
        name="hourly_market_reports",
        work_pool_name="local",
        cron="0 * * * *",
    )
