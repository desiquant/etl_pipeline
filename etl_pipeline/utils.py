import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from loguru import logger
from prefect import task
from prefect_shell import ShellOperation

load_dotenv(override=True)


aws_session = boto3.Session()

# Authenticate AWS with access keys
# aws_session = boto3.Session(
#     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
# )


s3 = aws_session.client(
    service_name="s3",
    endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL"),
)


def csv_to_parquet(input_paths: List[str], output_path: str):
    """Given a list of CSV files, converts them to a single parquet"""
    writer = None

    # create output parent dirs if does not exist
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    for i, file_path in enumerate(input_paths, 1):
        reader = pd.read_csv(file_path, chunksize=20000)

        for chunk in reader:
            table = pa.Table.from_pandas(chunk)

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)

            writer.write_table(table)

    if writer:
        writer.close()

    return output_path


@task
async def sync_s3(include: list[str] = []):
    local_dir = "./data/s3"  # TODO: make this dynamic path
    remote_dir = f"s3://{os.getenv('AWS_S3_BUCKET')}/data"

    include_flags = " ".join([f"--include '{i}'" for i in include])

    # TODO: make this dynamic path
    await ShellOperation(
        commands=[f"aws s3 sync {local_dir} {remote_dir} {include_flags} --size-only"]
    ).run()


def upload_folder_to_s3(local_folder: str, remote_folder: str):
    """Uploads an entire folder to S3"""
    local_folder = Path(local_folder)
    remote_folder = Path(remote_folder)

    print("uploading folder:", local_folder.absolute())

    with ThreadPoolExecutor() as e:
        # TODO: Currently, if an error in raised in upload_file_to_s3. The prefect flow is shown as success. We need to show failed threaded functions to prefect output.
        @logger.catch
        def upload_file_to_s3(file_path, s3_key):
            s3.upload_file(str(file_path), os.getenv("AWS_S3_BUCKET"), str(s3_key))
            print("uploaded:", file_path, s3_key)

        for file_path in local_folder.rglob("*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_folder)
                s3_key = remote_folder / relative_path.as_posix()

                e.submit(upload_file_to_s3, file_path, s3_key)

def number_of_new_entries(local_dir: str, file_path: str):
    """Find no of new entries for each of the symbols from the scraped announcements parquet"""

    markdown_dict = {}
    latest_dates = pd.read_csv(file_path,index_col=0)
    latest_dates['Date'] = pd.to_datetime(latest_dates['Date'],errors='coerce')
    
    for symbol in latest_dates.index:
        # Retrieve the last known entry date for the symbol
        last_known_entry = latest_dates.loc[symbol,'Date']
        
        parquet_file = Path(os.path.join(local_dir, f"{symbol}.parquet"))

        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            if pd.isna(last_known_entry):
                markdown_dict[symbol] = len(df)
                latest_dates.loc[symbol,'Date'] = df['date'].max() 
            else:
                # Filter for new entries
                df_new = df[df["date"] > last_known_entry]
                markdown_dict[symbol] = len(df_new)
                
                if not df_new.empty:
                    latest_dates.loc[symbol,'Date'] = df_new["date"].max()
        else:
            print(f"Warning: Parquet file for {symbol} does not exist.")
            markdown_dict[symbol] = 0

    latest_dates.to_csv(file_path)

    return markdown_dict

def update_csv_with_latest_dates(local_dir: str, file_path: str):
    """
    Loads the latest_dateInfo json, looks the data parquet to set the keys with the latest available dates, if data unavailable set the symbol with the default value.
    """
    # TODO: Might need to add parquet download from s3 incase of file unavailability locally
    latest_dates = pd.read_csv(file_path,index_col=0)
    latest_dates['Date'] = pd.to_datetime(latest_dates['Date'],errors='coerce')
    
    for symbol in latest_dates.index:
        parquet_file = Path(os.path.join(local_dir, f"{symbol}.parquet"))
        
        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            # Update with the latest date in the parquet file
            latest_dates.loc[symbol,'Date'] = df['date'].max() 
        else:
            print(f"No parquet file found for {symbol}. Setting default date.")
            latest_dates.loc[symbol,'Date'] = pd.NaT
    
    latest_dates.to_csv(file_path)

