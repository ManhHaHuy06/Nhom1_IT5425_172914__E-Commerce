# One-shot: Data Source API -> lakehouse infra -> crawler -> Spark Bronze/Silver/Gold -> ClickHouse.
# Usage:
#   .\run_lakehouse.ps1
#   .\run_lakehouse.ps1 -SkipCrawl          # infra + pipeline only (CSV already on MinIO)
#   .\run_lakehouse.ps1 -SkipInfra          # crawler + pipeline (stack already up)
#   .\run_lakehouse.ps1 -SkipDataSource -SkipInfra -SkipCrawl   # Spark + ClickHouse only
#   .\run_lakehouse.ps1 -SkipGold           # stop after Silver
#   .\run_lakehouse.ps1 -SkipClickHouse     # Gold Iceberg/export only, skip CH load
# Optional env:
#   OLIST_START_DATE / OLIST_END_DATE  -> also passed to crawler as START_DATE / END_DATE
#   OLIST_SOURCE_URI, OLIST_BRONZE_MODE, OLIST_BATCH_ID

param(
    [switch]$SkipDataSource,
    [switch]$SkipInfra,
    [switch]$SkipCrawl,
    [switch]$SkipGold,
    [switch]$SkipClickHouse
)

$ErrorActionPreference = "Stop"

$LakehouseRoot = $PSScriptRoot
$InfraDir = Join-Path $LakehouseRoot "infrastructure"
$CrawlerDir = Join-Path (Split-Path $LakehouseRoot -Parent) "olist-crawler-to-minio"
$DataSourceDir = Join-Path (Split-Path $LakehouseRoot -Parent) "olist-data-source-v2"

$SourceUri = if ($env:OLIST_SOURCE_URI) { $env:OLIST_SOURCE_URI } else { "s3a://handoff/bronze" }
$Mode = if ($env:OLIST_BRONZE_MODE) { $env:OLIST_BRONZE_MODE } else { "overwrite" }
$BatchId = $env:OLIST_BATCH_ID
$StartDate = $env:OLIST_START_DATE
$EndDate = $env:OLIST_END_DATE

# Align crawler date env with lakehouse filter when set.
if ($StartDate -and -not $env:START_DATE) { $env:START_DATE = $StartDate }
if ($EndDate -and -not $env:END_DATE) { $env:END_DATE = $EndDate }

function Assert-LastExit([string]$Context) {
    if ($LASTEXITCODE -ne 0) { throw "$Context failed (exit $LASTEXITCODE)" }
}

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [int]$TimeoutSec = 180
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $Name 2>$null
        if ($LASTEXITCODE -eq 0 -and ($status -eq "healthy" -or $status -eq "running")) {
            Write-Host "  $Name : $status"
            return
        }
        Start-Sleep -Seconds 5
    }
    throw "Timed out waiting for $Name to become healthy"
}

function Invoke-SparkStage {
    param(
        [Parameter(Mandatory = $true)][string]$Stage,
        [string]$StageMode = $Mode
    )

    $sparkArgs = @(
        "exec", "olist-spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "/opt/spark/olist_pipeline.py",
        "--stage", $Stage,
        "--layers-dir", "/opt/spark/layers",
        "--mode", $StageMode,
        "--source-uri", $SourceUri
    )

    if ($BatchId) { $sparkArgs += @("--batch-id", $BatchId) }
    if ($StartDate) { $sparkArgs += @("--start-date", $StartDate) }
    if ($EndDate) { $sparkArgs += @("--end-date", $EndDate) }

    Write-Host "=== $Stage ==="
    & docker @sparkArgs
    Assert-LastExit "Stage $Stage"
}

function Test-DataSourceUp {
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 3
        return ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500)
    }
    catch { return $false }
}

# --- 1) Data Source API (:8000) ---
if (-not $SkipDataSource) {
    if (Test-DataSourceUp) {
        Write-Host "=== Data source already up on :8000 (skip build) ==="
    }
    elseif (-not (Test-Path $DataSourceDir)) {
        Write-Warning "Data source folder not found: $DataSourceDir (skipping)"
    }
    else {
        Write-Host "=== Start data source API ==="
        Push-Location $DataSourceDir
        try {
            # Prefer existing local image when Docker Hub DNS is flaky (--build needs registry).
            $localImg = docker images -q "olist-data-source-v2-olist-data-source:latest"
            if ($localImg) {
                docker compose up -d --no-build
            }
            else {
                docker compose up -d --build
            }
            Assert-LastExit "Data source compose up"
        }
        finally { Pop-Location }
        $deadline = (Get-Date).AddSeconds(90)
        while ((Get-Date) -lt $deadline) {
            if (Test-DataSourceUp) { break }
            Start-Sleep -Seconds 3
        }
        if (-not (Test-DataSourceUp)) {
            throw "Data source not reachable on http://localhost:8000"
        }
        Write-Host "Data source ready on http://localhost:8000"
    }
}

# --- 2) Lakehouse infra ---
if (-not $SkipInfra) {
    Write-Host "=== Start lakehouse infrastructure ==="
    Push-Location $InfraDir
    try {
        if (-not (Test-Path ".env")) {
            if (Test-Path ".env.example") {
                Copy-Item ".env.example" ".env"
                Write-Host "Created infrastructure/.env from .env.example"
            }
            else {
                throw "Missing infrastructure/.env (and no .env.example)"
            }
        }
        docker compose up -d --build
        Assert-LastExit "Lakehouse compose up"
    }
    finally { Pop-Location }

    Write-Host "Waiting for core services..."
    Wait-ContainerHealthy -Name "olist-minio"
    Wait-ContainerHealthy -Name "olist-iceberg-rest"
    Wait-ContainerHealthy -Name "olist-spark-master"
    Wait-ContainerHealthy -Name "olist-trino"
    Wait-ContainerHealthy -Name "olist-clickhouse"
}

# --- 3) Crawler -> shared MinIO bronze/ ---
if (-not $SkipCrawl) {
    if (-not (Test-Path $CrawlerDir)) {
        throw "Crawler folder not found: $CrawlerDir"
    }
    Write-Host "=== Run crawler (olist-crawler-to-minio) ==="
    Push-Location $CrawlerDir
    try {
        docker compose up --build --abort-on-container-exit --exit-code-from olist-crawler
        Assert-LastExit "Crawler"
    }
    finally { Pop-Location }
    Write-Host "Crawler finished"
}

# --- 4) Spark pipeline ---
# ddl: create Iceberg namespaces/tables
# bronze: load CSV handoff from s3a://handoff/bronze
# silver-dimensions / silver-facts: clean + star schema (+ A1-A6 timeline filter)
# gold: business marts + Parquet export under s3a://lakehouse/exports/gold/
$stages = @("ddl", "bronze", "silver-dimensions", "silver-facts")
if (-not $SkipGold) {
    $stages += "gold"
}
foreach ($stage in $stages) {
    Invoke-SparkStage -Stage $stage
}

# --- 5) ClickHouse serving load (needs Gold Parquet export) ---
if (-not $SkipGold -and -not $SkipClickHouse) {
    Write-Host "=== Load Gold into ClickHouse ==="
    if (-not $SkipInfra) {
        Wait-ContainerHealthy -Name "olist-clickhouse"
    }
    $chPassword = "admin123"
    $envFile = Join-Path $InfraDir ".env"
    if (Test-Path $envFile) {
        $match = Select-String -Path $envFile -Pattern '^\s*CLICKHOUSE_PASSWORD=(.+)$' | Select-Object -First 1
        if ($match) {
            $chPassword = $match.Matches[0].Groups[1].Value.Trim().Trim('"').Trim("'")
        }
    }
    docker exec olist-clickhouse bash -c "clickhouse-client --password '$chPassword' --multiquery < /opt/olist/load_gold.sql"
    Assert-LastExit "ClickHouse load_gold"
    Write-Host "ClickHouse olist_gold.* reloaded"
}

Write-Host "Full run finished (infra + crawl + Bronze/Silver/Gold + ClickHouse)"
