# Server setup

- Volume Mount
- Prefect Server
  - prefect config set PREFECT_API_URL=http://0.0.0.0:4200/api
- Minio Service + Configuration - Access Keys, Secrets, etc.
- Nginx

  Setup up SSL:

  `sudo apt install certbot python3-certbot-nginx`
  `sudo certbot --nginx -d data.desiquant.com -d www.data.desiquant.com`
