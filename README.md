# DesiQuant ETL Pipeline

## TODO

- There is problem with relative imports. `from utils import csv_to_parquet` works while running locally but when prefect server executes a flow, it needs to be `from etl_pipeline.utils import csv_to_parquet`. Or else it fails with the following error: `ModuleNotFoundError: No module named 'utils'`
- Remove the columns headers that are also included when combining multiple CSV to a single parquet file. This was not a problem with .jl files. For eg. You'll find rows in the dataframe with `article_text	author	date_modified	date_published	description	scrapy_parsed_at	scrapy_scraped_at	title	url`

## Server setup

- Volume Mount
- Prefect Server
  - prefect config set PREFECT_API_URL=http://0.0.0.0:4200/api
- Minio Service + Configuration - Access Keys, Secrets, etc.
- Nginx
- Add services to systemctl
- Make sure the systemd services user has the necessary config files used for each library like `~/.prefect`, `~/.aws` etc.

  Setup up SSL:

  `sudo apt install certbot python3-certbot-nginx`
  `sudo certbot --nginx -d data.desiquant.com -d www.data.desiquant.com`
