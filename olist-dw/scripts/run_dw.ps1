$ErrorActionPreference = "Stop"

Write-Host "=== Starting MySQL DW ==="
docker compose up -d mysql-dw

Write-Host "=== MySQL status ==="
docker compose ps mysql-dw

Write-Host "=== Loading CURATED -> MySQL DW ==="
docker compose run --rm dw-loader

Write-Host "=== DW load finished ==="
