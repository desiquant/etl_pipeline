# DesiQuant ETL Pipeline

## TODO

- Better organization of environment variables. There are too many free parameters that are being configured. Data directly for `scrapy` is being used from `scrapy.cfg`, for the `scraper` is being used `DATA_STORAGE_LOCAL_PATH` . Also there is no clear indication which AWS secrets are being used and being loaded from where. This needs better clarity overall. A centralized way to define all important variables
- There is problem with relative imports. `from utils import csv_to_parquet` works while running locally but when prefect server executes a flow, it needs to be `from etl_pipeline.utils import csv_to_parquet`. Or else it fails with the following error: `ModuleNotFoundError: No module named 'utils'`
- Remove the columns headers that are also included when combining multiple CSV to a single parquet file. This was not a problem with .jl files. For eg. You'll find rows in the dataframe with `article_text	author	date_modified	date_published	description	scrapy_parsed_at	scrapy_scraped_at	title	url`

## Server setup

- Volume Mount
- Prefect Server
- - Install `aws-cli` since flows use `aws s3 sync` to upload data
  - `prefect config set PREFECT_API_URL=http://0.0.0.0:4200/api`
  - Background:
    - Start Server: `prefect server start` or use the systemd service
    - Start Worker: `prefect worker start --pool 'ar-workpool-local'` or use the systemd service. Make sure it uses your `.venv`
  - After making changes:
    - Deploy: `prefect deploy` - Deploys the flows. The worker polls tasks from it and processes them.
- Minio Service + Configuration - Access Keys, Secrets, etc.
- Nginx
- Add services to systemctl
- Make sure the systemd services user has the necessary config files used for each library like `~/.prefect`, `~/.aws` etc.

  Setup up SSL:

  `sudo apt install certbot python3-certbot-nginx`
  `sudo certbot --nginx -d data.desiquant.com -d www.data.desiquant.com`
