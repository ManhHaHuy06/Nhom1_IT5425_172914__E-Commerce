param(
    [string]$IntegrationEnvPath = ""
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not (Test-Path ".env")) {
    if (Test-Path "infrastructure/.env") {
        Copy-Item "infrastructure/.env" ".env"
        Write-Host "Copied existing Lakehouse infrastructure/.env into root .env (values were not displayed)."
    } else {
        Copy-Item ".env.example" ".env"
        Write-Warning "Created .env from .env.example. Match existing Lakehouse volume credentials before starting."
    }
}

# Merge only OLTP credentials from the previous Integration environment so its MySQL volume can be reused.
if (-not $IntegrationEnvPath) {
    $candidate = Join-Path $root "..\intergration_hosting_sever_completed_v2\intergration_hosting_sever\Data_Viz\.env"
    if (Test-Path $candidate) { $IntegrationEnvPath = $candidate }
}
if ($IntegrationEnvPath -and (Test-Path $IntegrationEnvPath)) {
    $keysToImport = @(
        "OLTP_ROOT_PASSWORD", "OLTP_DATABASE", "OLTP_SIM_USER", "OLTP_SIM_PASSWORD",
        "OLTP_ETL_USER", "OLTP_ETL_PASSWORD", "OLTP_PORT"
    )
    $imported = @{}
    foreach ($line in Get-Content $IntegrationEnvPath) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
            $key = $Matches[1]
            if ($keysToImport -contains $key) { $imported[$key] = $Matches[2] }
        }
    }
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.AddRange([string[]](Get-Content ".env"))
    foreach ($key in $keysToImport) {
        if ($imported.ContainsKey($key)) {
            $found = $false
            for ($i = 0; $i -lt $lines.Count; $i++) {
                if ($lines[$i] -match "^\s*$([regex]::Escape($key))\s*=") {
                    $lines[$i] = "$key=$($imported[$key])"
                    $found = $true
                }
            }
            if (-not $found) { $lines.Add("$key=$($imported[$key])") }
        }
    }
    Set-Content -Path ".env" -Value $lines -Encoding utf8
    Write-Host "Imported available OLTP settings from the previous Integration .env (values were not displayed)."
}

$existing = @{}
Get-Content ".env" | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=') { $existing[$Matches[1]] = $true }
}
$missing = @()
Get-Content ".env.example" | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=') {
        $key = $Matches[1]
        if (-not $existing.ContainsKey($key)) {
            $missing += $_
            $existing[$key] = $true
        }
    }
}
if ($missing.Count -gt 0) {
    Add-Content -Path ".env" -Value "`n# Added unified Integration settings"
    Add-Content -Path ".env" -Value $missing
    Write-Host "Added missing configuration keys to .env: $($missing.Count). Review placeholders without displaying secrets."
}
Write-Host "Environment file prepared. Confirm Lakehouse and OLTP credentials match existing persistent volumes."
