@echo off
setlocal EnableDelayedExpansion

rem Olist lakehouse runner.  Usage: run_lakehouse.bat [stage ...]   (see :help)
rem Only the listed stages run, always in pipeline order. No stage = all.
rem Bronze source: Parquet landed by hosting_server Airflow in s3a://olist-lake/bronze (SOURCE_S3_* in infrastructure/.env).

set "PIPELINE=infra ddl bronze silver-dimensions silver-facts gold clickhouse"
rem Script folder: read %~dp0 before SHIFT (SHIFT also moves %0, after which %~dp0 is the current directory)
set "Root=%~dp0"
if "%Root:~-1%"=="\" set "Root=%Root:~0,-1%"
set "InfraDir=%Root%\infrastructure"

if "%~1"=="" for %%S in (%PIPELINE%) do set "Run_%%S=1"

:parse_args
if "%~1"=="" goto run
set "Arg=%~1"
rem "/?" cannot go in a FOR list (? is a filename wildcard there)
if "!Arg!"=="/?" goto help
for %%H in (help -h --help) do if /i "!Arg!"=="%%H" goto help
if /i "!Arg!"=="all" (
    for %%S in (%PIPELINE%) do set "Run_%%S=1"
    goto next_arg
)
if /i "!Arg!"=="silver" (
    set "Run_silver-dimensions=1"
    set "Run_silver-facts=1"
    goto next_arg
)
if /i "!Arg!"=="silver-dims" set "Arg=silver-dimensions"
set "Known=0"
for %%S in (%PIPELINE%) do if /i "!Arg!"=="%%S" (
    set "Run_%%S=1"
    set "Known=1"
)
if "!Known!"=="0" (
    echo Unknown stage: %~1
    echo Valid: %PIPELINE% silver all  ^(alias: silver-dims^)
    exit /b 1
)
:next_arg
shift
goto parse_args

:run
if "%OLIST_SOURCE_URI%"=="" (set "SourceUri=s3a://olist-lake/bronze") else (set "SourceUri=%OLIST_SOURCE_URI%")
if "%OLIST_BRONZE_MODE%"=="" (set "Mode=overwrite") else (set "Mode=%OLIST_BRONZE_MODE%")

echo Bronze source: %SourceUri%
echo Selected stages ^(pipeline order^):
for %%S in (%PIPELINE%) do if defined Run_%%S echo   - %%S

for %%S in (%PIPELINE%) do if defined Run_%%S (
    if "%%S"=="infra" (
        call :infra
    ) else if "%%S"=="clickhouse" (
        call :clickhouse
    ) else (
        call :spark %%S
    )
    if errorlevel 1 exit /b 1
)
echo Run finished
exit /b 0

rem ---------------------------------------------------------------- stages
:infra
echo === Start lakehouse infrastructure ===
if not exist "%InfraDir%\.env" (
    if not exist "%InfraDir%\.env.example" (
        echo Missing infrastructure\.env ^(and no .env.example^)
        exit /b 1
    )
    copy "%InfraDir%\.env.example" "%InfraDir%\.env" >nul
    echo Created infrastructure\.env from .env.example
)
pushd "%InfraDir%"
docker compose up -d --build
set "ComposeError=%ERRORLEVEL%"
popd
if not "%ComposeError%"=="0" (
    echo Lakehouse compose up failed
    exit /b 1
)
echo Waiting for core services...
for %%C in (olist-minio olist-iceberg-rest olist-spark-master olist-trino olist-clickhouse) do (
    call :wait_container %%C
    if errorlevel 1 exit /b 1
)
exit /b 0

:spark
echo === %~1 ===
set "SparkArgs=--stage %~1 --layers-dir /opt/spark/layers --mode %Mode% --source-uri %SourceUri%"
if not "%OLIST_BATCH_ID%"=="" set "SparkArgs=!SparkArgs! --batch-id %OLIST_BATCH_ID%"
if not "%OLIST_START_DATE%"=="" set "SparkArgs=!SparkArgs! --start-date %OLIST_START_DATE%"
if not "%OLIST_END_DATE%"=="" set "SparkArgs=!SparkArgs! --end-date %OLIST_END_DATE%"
docker exec olist-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/olist_pipeline.py !SparkArgs!
if errorlevel 1 (
    echo Stage %~1 failed
    exit /b 1
)
exit /b 0

:clickhouse
echo === Load Gold into ClickHouse ===
call :wait_container olist-clickhouse
if errorlevel 1 exit /b 1
rem clickhouse-client reads CLICKHOUSE_PASSWORD from the container environment (infrastructure/.env).
rem Schema first (recreates staging tables *__new from the latest Gold DDL), then load them and EXCHANGE into the serving tables.
docker exec -i olist-clickhouse clickhouse-client --multiquery < "%Root%\layers\clickhouse\init_gold.sql"
if errorlevel 1 (
    echo ClickHouse init_gold failed
    exit /b 1
)
docker exec olist-clickhouse bash -c "clickhouse-client --multiquery < /opt/olist/load_gold.sql"
if errorlevel 1 (
    echo ClickHouse load_gold failed
    exit /b 1
)
echo ClickHouse olist_gold.* reloaded
exit /b 0

rem ---------------------------------------------------------------- helpers
:wait_container
rem Wait until a container is healthy (or running when it has no healthcheck); 36 x 5s.
for /l %%I in (1,1,36) do (
    set "Status="
    for /f "tokens=*" %%S in ('docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" %~1 2^>nul') do set "Status=%%S"
    if "!Status!"=="healthy" goto container_ready
    if "!Status!"=="running" goto container_ready
    rem ping as a 5s delay: TIMEOUT fails at once when stdin is not a console (CI, IDE, PowerShell -NonInteractive)
    ping -n 6 127.0.0.1 >nul
)
echo Timed out waiting for %~1 to become healthy
exit /b 1
:container_ready
echo   %~1 : !Status!
exit /b 0

:help
echo Usage: run_lakehouse.bat [stage ...]
echo.
echo No args  = run full pipeline
echo Stages   = only listed stages run ^(order is always pipeline order^)
echo.
echo Stages:
echo   infra               Start lakehouse Docker infra
echo   ddl                 Spark Iceberg DDL
echo   bronze              Spark Bronze load from hosting_server Parquet ^(start hosting_server first^)
echo   silver-dimensions   Spark Silver dimensions   ^(alias: silver-dims^)
echo   silver-facts        Spark Silver facts
echo   silver              silver-dimensions + silver-facts
echo   gold                Spark Gold marts + Parquet export
echo   clickhouse          Load Gold Parquet into ClickHouse
echo   all                 Same as no args
echo.
echo Environment: OLIST_SOURCE_URI, OLIST_BRONZE_MODE ^(overwrite^|append^), OLIST_BATCH_ID,
echo              OLIST_START_DATE / OLIST_END_DATE ^(YYYY-MM-DD, filters Bronze ingest_date, needs OLIST_BRONZE_MODE=append^)
echo.
echo Examples:
echo   run_lakehouse.bat
echo   run_lakehouse.bat ddl gold clickhouse
echo   run_lakehouse.bat bronze silver gold
echo   run_lakehouse.bat clickhouse
exit /b 0
