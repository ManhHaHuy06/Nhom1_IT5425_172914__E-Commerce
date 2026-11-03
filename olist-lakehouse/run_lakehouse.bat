@echo off
setlocal EnableDelayedExpansion

rem Stages (whitelist): khong truyen arg = chay tat ca.
rem Thu tu thuc thi luon theo pipeline, khong theo thu tu arg.
rem Stages: datasource infra crawl ddl bronze silver-dimensions silver-facts gold clickhouse
rem Alias: silver (= silver-dimensions + silver-facts), all, help

set "RunDataSource=0"
set "RunInfra=0"
set "RunCrawl=0"
set "RunDdl=0"
set "RunBronze=0"
set "RunSilverDimensions=0"
set "RunSilverFacts=0"
set "RunGold=0"
set "RunClickHouse=0"
set "RunAll=0"
set "HasStageArg=0"

:parse_args
if "%~1"=="" goto done_args
if /i "%~1"=="help" goto show_help
if /i "%~1"=="-h" goto show_help
if /i "%~1"=="--help" goto show_help
if /i "%~1"=="/?" goto show_help

set "HasStageArg=1"
if /i "%~1"=="all" (
    set "RunAll=1"
    shift
    goto parse_args
)
if /i "%~1"=="datasource" (
    set "RunDataSource=1"
    goto next_arg
)
if /i "%~1"=="data-source" (
    set "RunDataSource=1"
    goto next_arg
)
if /i "%~1"=="infra" (
    set "RunInfra=1"
    goto next_arg
)
if /i "%~1"=="crawl" (
    set "RunCrawl=1"
    goto next_arg
)
if /i "%~1"=="ddl" (
    set "RunDdl=1"
    goto next_arg
)
if /i "%~1"=="bronze" (
    set "RunBronze=1"
    goto next_arg
)
if /i "%~1"=="silver-dimensions" (
    set "RunSilverDimensions=1"
    goto next_arg
)
if /i "%~1"=="silver-dims" (
    set "RunSilverDimensions=1"
    goto next_arg
)
if /i "%~1"=="silver-facts" (
    set "RunSilverFacts=1"
    goto next_arg
)
if /i "%~1"=="silver" (
    set "RunSilverDimensions=1"
    set "RunSilverFacts=1"
    goto next_arg
)
if /i "%~1"=="gold" (
    set "RunGold=1"
    goto next_arg
)
if /i "%~1"=="clickhouse" (
    set "RunClickHouse=1"
    goto next_arg
)

echo Unknown stage: %~1
echo Valid: datasource infra crawl ddl bronze silver-dimensions silver-facts silver gold clickhouse all
echo        ^(aliases: data-source, silver-dims^)
exit /b 1

:next_arg
shift
goto parse_args

:done_args
if "%HasStageArg%"=="0" set "RunAll=1"
if "%RunAll%"=="1" (
    set "RunDataSource=1"
    set "RunInfra=1"
    set "RunCrawl=1"
    set "RunDdl=1"
    set "RunBronze=1"
    set "RunSilverDimensions=1"
    set "RunSilverFacts=1"
    set "RunGold=1"
    set "RunClickHouse=1"
)

set "LakehouseRoot=%~dp0"
if "%LakehouseRoot:~-1%"=="\" set "LakehouseRoot=%LakehouseRoot:~0,-1%"
set "InfraDir=%LakehouseRoot%\infrastructure"
for %%I in ("%LakehouseRoot%\..") do set "ParentDir=%%~fI"
set "CrawlerDir=%ParentDir%\olist-crawler-to-minio"
set "DataSourceDir=%ParentDir%\olist-data-source-v2"

if "%OLIST_SOURCE_URI%"=="" (set "SourceUri=s3a://handoff/bronze") else (set "SourceUri=%OLIST_SOURCE_URI%")
if "%OLIST_BRONZE_MODE%"=="" (set "Mode=overwrite") else (set "Mode=%OLIST_BRONZE_MODE%")
set "BatchId=%OLIST_BATCH_ID%"
set "StartDate=%OLIST_START_DATE%"
set "EndDate=%OLIST_END_DATE%"

if not "%StartDate%"=="" if "%START_DATE%"=="" set "START_DATE=%StartDate%"
if not "%EndDate%"=="" if "%END_DATE%"=="" set "END_DATE=%EndDate%"

echo Selected stages ^(pipeline order^):
if "%RunDataSource%"=="1" echo   - datasource
if "%RunInfra%"=="1" echo   - infra
if "%RunCrawl%"=="1" echo   - crawl
if "%RunDdl%"=="1" echo   - ddl
if "%RunBronze%"=="1" echo   - bronze
if "%RunSilverDimensions%"=="1" echo   - silver-dimensions
if "%RunSilverFacts%"=="1" echo   - silver-facts
if "%RunGold%"=="1" echo   - gold
if "%RunClickHouse%"=="1" echo   - clickhouse

rem --- 1) Data Source API (:8000) ---
if "%RunDataSource%"=="1" (
    call :test_datasource
    if !ERRORLEVEL! equ 0 (
        echo === Data source already up on :8000 (skip build) ===
    ) else if not exist "%DataSourceDir%" (
        echo Data source folder not found: %DataSourceDir% (skipping)
    ) else (
        echo === Start data source API ===
        pushd "%DataSourceDir%"
        for /f "tokens=*" %%I in ('docker images -q olist-data-source-v2-olist-data-source:latest 2^>nul') do set "LocalImg=%%I"
        if not "!LocalImg!"=="" (
            docker compose up -d --no-build
        ) else (
            docker compose up -d --build
        )
        if errorlevel 1 (
            echo Data source compose up failed
            popd
            exit /b 1
        )
        popd

        set /a DS_WAIT=0
        :wait_ds_loop
        call :test_datasource
        if !ERRORLEVEL! equ 0 goto ds_ready
        timeout /t 3 /nobreak >nul
        set /a DS_WAIT+=3
        if !DS_WAIT! lss 90 goto wait_ds_loop

        call :test_datasource
        if !ERRORLEVEL! neq 0 (
            echo Data source not reachable on http://localhost:8000
            exit /b 1
        )
        :ds_ready
        echo Data source ready on http://localhost:8000
    )
)

rem --- 2) Lakehouse infra ---
if "%RunInfra%"=="1" (
    echo === Start lakehouse infrastructure ===
    pushd "%InfraDir%"
    if not exist ".env" (
        if exist ".env.example" (
            copy ".env.example" ".env" >nul
            echo Created infrastructure/.env from .env.example
        ) else (
            echo Missing infrastructure/.env ^(and no .env.example^)
            popd
            exit /b 1
        )
    )
    docker compose up -d --build
    if errorlevel 1 (
        echo Lakehouse compose up failed
        popd
        exit /b 1
    )
    popd

    echo Waiting for core services...
    call :wait_container olist-minio || exit /b 1
    call :wait_container olist-iceberg-rest || exit /b 1
    call :wait_container olist-spark-master || exit /b 1
    call :wait_container olist-trino || exit /b 1
    call :wait_container olist-clickhouse || exit /b 1
)

rem --- 3) Crawler -> shared MinIO bronze/ ---
if "%RunCrawl%"=="1" (
    if not exist "%CrawlerDir%" (
        echo Crawler folder not found: %CrawlerDir%
        exit /b 1
    )
    echo === Run crawler ^(olist-crawler-to-minio^) ===
    pushd "%CrawlerDir%"
    docker compose up --build --abort-on-container-exit --exit-code-from olist-crawler
    if errorlevel 1 (
        echo Crawler failed
        popd
        exit /b 1
    )
    popd
    echo Crawler finished
)

rem --- 4) Spark pipeline ---
if "%RunDdl%"=="1" (
    call :invoke_spark ddl
    if errorlevel 1 exit /b 1
)
if "%RunBronze%"=="1" (
    call :invoke_spark bronze
    if errorlevel 1 exit /b 1
)
if "%RunSilverDimensions%"=="1" (
    call :invoke_spark silver-dimensions
    if errorlevel 1 exit /b 1
)
if "%RunSilverFacts%"=="1" (
    call :invoke_spark silver-facts
    if errorlevel 1 exit /b 1
)
if "%RunGold%"=="1" (
    call :invoke_spark gold
    if errorlevel 1 exit /b 1
)

rem --- 5) ClickHouse serving load ---
if "%RunClickHouse%"=="1" (
    echo === Load Gold into ClickHouse ===
    call :wait_container olist-clickhouse
    if errorlevel 1 exit /b 1
    set "CH_PASSWORD=admin123"
    if exist "%InfraDir%\.env" (
        for /f "usebackq tokens=1,* delims==" %%A in ("%InfraDir%\.env") do (
            set "VAR_NAME=%%A"
            for /f "tokens=* delims= " %%K in ("!VAR_NAME!") do set "VAR_NAME=%%K"
            if "!VAR_NAME!"=="CLICKHOUSE_PASSWORD" (
                set "CH_VAL=%%B"
                for /f "tokens=* delims= " %%V in ("!CH_VAL!") do set "CH_VAL=%%V"
                set "CH_PASSWORD=!CH_VAL!"
            )
        )
    )
    docker exec olist-clickhouse bash -c "clickhouse-client --password '!CH_PASSWORD!' --multiquery < /opt/olist/load_gold.sql"
    if errorlevel 1 (
        echo ClickHouse load_gold failed
        exit /b 1
    )
    echo ClickHouse olist_gold.* reloaded
)

echo Run finished
exit /b 0

:show_help
echo Usage: run_lakehouse.bat [stage ...]
echo.
echo No args  = run full pipeline
echo Stages   = only listed stages run ^(order is always pipeline order^)
echo.
echo Stages:
echo   datasource   Start Data Source API :8000
echo   infra        Start lakehouse Docker infra
echo   crawl        Run crawler to MinIO handoff/bronze
echo   ddl          Spark Iceberg DDL
echo   bronze       Spark Bronze load
echo   silver-dimensions   Spark Silver dimensions
echo   silver-facts        Spark Silver facts
echo   silver       Alias for silver-dimensions + silver-facts
echo   gold         Spark Gold marts + Parquet export
echo   clickhouse   Load Gold Parquet into ClickHouse
echo   all          Same as no args
echo.
echo Aliases: data-source, silver-dims
echo.
echo Examples:
echo   run_lakehouse.bat
echo   run_lakehouse.bat ddl gold clickhouse
echo   run_lakehouse.bat bronze silver gold
echo   run_lakehouse.bat clickhouse
exit /b 0

rem --- Ham kiem tra container healthy hoac running ---
:wait_container
set "C_NAME=%~1"
set /a ATTEMPTS=0
:wait_c_loop
set "STATUS="
for /f "tokens=*" %%S in ('docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" %C_NAME% 2^>nul') do set "STATUS=%%S"
if "%STATUS%"=="healthy" (
    echo   %C_NAME% : %STATUS%
    exit /b 0
)
if "%STATUS%"=="running" (
    echo   %C_NAME% : %STATUS%
    exit /b 0
)
set /a ATTEMPTS+=1
if !ATTEMPTS! geq 36 (
    echo Timed out waiting for %C_NAME% to become healthy
    exit /b 1
)
timeout /t 5 /nobreak >nul
goto wait_c_loop

rem --- Ham chay Spark stage ---
:invoke_spark
set "STAGE=%~1"
echo === %STAGE% ===
set "SPARK_EXTRA="
if not "%BatchId%"=="" set SPARK_EXTRA=!SPARK_EXTRA! --batch-id %BatchId%
if not "%StartDate%"=="" set SPARK_EXTRA=!SPARK_EXTRA! --start-date %StartDate%
if not "%EndDate%"=="" set SPARK_EXTRA=!SPARK_EXTRA! --end-date %EndDate%

docker exec olist-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/olist_pipeline.py --stage %STAGE% --layers-dir /opt/spark/layers --mode %Mode% --source-uri %SourceUri%!SPARK_EXTRA!
if errorlevel 1 (
    echo Stage %STAGE% failed ^(exit %ERRORLEVEL%^)
    exit /b 1
)
exit /b 0

rem --- Ham kiem tra Data Source API ---
:test_datasource
curl.exe -s -f -o nul --connect-timeout 3 http://localhost:8000/docs >nul 2>&1
exit /b %ERRORLEVEL%
