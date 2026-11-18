@echo off
setlocal
cd /d "%~dp0"
echo Starting the unified Olist Lakehouse stack.
echo Make sure .env contains the credentials of your existing Lakehouse volumes.
if not exist ".env" (
  echo .env not found. Run setup.ps1 and edit .env first.
  exit /b 1
)
docker compose config --quiet
if errorlevel 1 (
  echo Docker Compose configuration is invalid. Check .env and docker-compose.yml.
  exit /b 1
)
docker compose up -d --build
if errorlevel 1 exit /b 1
echo Unified Olist Lakehouse started. Airflow: http://localhost:8080 Dashboard: http://localhost:8501
exit /b 0
