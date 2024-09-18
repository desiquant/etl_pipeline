import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def jl_to_parquet(input_paths: List[str], output_path: str):
    """Given a list of JSON lines files, converts them to parquet"""
    writer = None

    # create output parent dirs if does not exist
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    for i, file_path in enumerate(input_paths, 1):
        reader = pd.read_json(file_path, lines=True, chunksize=20000)

        for chunk in reader:
            table = pa.Table.from_pandas(chunk)

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)

            writer.write_table(table)

    if writer:
        writer.close()

    return output_path


def upload_folder_to_s3(local_folder: str, remote_folder: str):
    """Uploads an entire folder to S3"""
    local_folder = Path(local_folder)
    remote_folder = Path(remote_folder)

    load_dotenv(override=True)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL"),
    )

    with ThreadPoolExecutor() as e:

        def upload_file_to_s3(file_path, s3_key):
            s3.upload_file(str(file_path), os.getenv("AWS_S3_BUCKET"), str(s3_key))
            print("uploaded:", file_path, s3_key)

        for file_path in local_folder.rglob("*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_folder)
                s3_key = remote_folder / relative_path.as_posix()

                e.submit(upload_file_to_s3, file_path, s3_key)
