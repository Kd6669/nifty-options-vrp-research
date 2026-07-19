param(
    [Parameter(Mandatory = $true)]
    [string]$Expiry,
    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [string]$RedisUrl = "redis://localhost:6379/0",
    [string]$StopAt = "",
    [double]$Spot = 0,
    [string]$Python = "py"
)

$ErrorActionPreference = "Stop"

if (-not $env:DHAN_ACCESS_TOKEN) {
    throw "DHAN_ACCESS_TOKEN must be set in the environment."
}

if (-not $StopAt) {
    $StopAt = "${Date}T15:35:00+05:30"
}

$root = (Resolve-Path ".").Path
$logDir = Join-Path $root "reports\dhan_live_${Date}_logs"
$restDir = Join-Path $root "reports\dhan_live_${Date}_rest"
$tbtDir = Join-Path $root "reports\dhan_live_${Date}_tbt"
$restStream = "stream:dhan_rest_full_day_$($Date.Replace('-', ''))"
$tbtStream = "stream:dhan_tbt_full_day_$($Date.Replace('-', ''))"
New-Item -ItemType Directory -Force -Path $logDir, $restDir, $tbtDir | Out-Null

$restScript = {
    param($Python, $Expiry, $RedisUrl, $Stream, $OutDir, $StopAt, $LogDir)
    while ([datetimeoffset]::Now -lt [datetimeoffset]::Parse($StopAt)) {
        $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $attemptDir = Join-Path $OutDir "attempt_$stamp"
        New-Item -ItemType Directory -Force -Path $attemptDir | Out-Null
        & $Python -m dhan_data_fetch_stream.cli capture-rest-redis `
            --expiry $Expiry `
            --iterations 0 `
            --interval-seconds 3.2 `
            --redis-url $RedisUrl `
            --redis-stream $Stream `
            --parquet-dir $attemptDir `
            --parquet-flush-rows 10000 `
            --maxlen 1000000 `
            1>> (Join-Path $LogDir "rest_${stamp}_stdout.log") `
            2>> (Join-Path $LogDir "rest_${stamp}_stderr.log")
        Start-Sleep -Seconds 30
    }
}

$tbtScript = {
    param($Python, $Expiry, $RedisUrl, $Stream, $OutDir, $StopAt, $LogDir, $Spot)
    while ([datetimeoffset]::Now -lt [datetimeoffset]::Parse($StopAt)) {
        $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $attemptDir = Join-Path $OutDir "attempt_$stamp"
        New-Item -ItemType Directory -Force -Path $attemptDir | Out-Null
        $args = @(
            "-m", "dhan_data_fetch_stream.cli", "capture-tbt-redis",
            "--expiry", $Expiry,
            "--full-chain",
            "--iterations", "0",
            "--max-no-update-seconds", "300",
            "--startup-timeout-seconds", "30",
            "--redis-url", $RedisUrl,
            "--redis-stream", $Stream,
            "--parquet-dir", $attemptDir,
            "--parquet-flush-rows", "10000",
            "--maxlen", "1000000"
        )
        if ($Spot -gt 0) {
            $args += @("--spot", [string]$Spot)
        }
        & $Python @args `
            1>> (Join-Path $LogDir "tbt_${stamp}_stdout.log") `
            2>> (Join-Path $LogDir "tbt_${stamp}_stderr.log")
        Start-Sleep -Seconds 30
    }
}

$restJob = Start-Job -Name "dhan-rest-$Date" -ScriptBlock $restScript -ArgumentList $Python, $Expiry, $RedisUrl, $restStream, $restDir, $StopAt, $logDir
$tbtJob = Start-Job -Name "dhan-tbt-$Date" -ScriptBlock $tbtScript -ArgumentList $Python, $Expiry, $RedisUrl, $tbtStream, $tbtDir, $StopAt, $logDir, $Spot

[pscustomobject]@{
    rest_job_id = $restJob.Id
    tbt_job_id = $tbtJob.Id
    rest_stream = $restStream
    tbt_stream = $tbtStream
    rest_dir = $restDir
    tbt_dir = $tbtDir
    log_dir = $logDir
    stop_at = $StopAt
} | ConvertTo-Json -Depth 3
