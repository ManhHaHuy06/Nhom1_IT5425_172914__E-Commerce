$ErrorActionPreference = "Stop"
docker compose up -d --build
docker compose run --rm spark-etl
