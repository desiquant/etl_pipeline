# DesiQuant ETL Pipeline

## TODO

- There is problem with relative imports. `from utils import csv_to_parquet` works while running locally but when prefect server executes a flow, it needs to be `from etl_pipeline.utils import csv_to_parquet`. Or else it fails with the following error: `ModuleNotFoundError: No module named 'utils'`

## Server setup

- Volume Mount
- Prefect Server
  - prefect config set PREFECT_API_URL=http://0.0.0.0:4200/api
- Minio Service + Configuration - Access Keys, Secrets, etc.
- Nginx

  Setup up SSL:

  `sudo apt install certbot python3-certbot-nginx`
  `sudo certbot --nginx -d data.desiquant.com -d www.data.desiquant.com`
