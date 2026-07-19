[CmdletBinding()]
param(
    [string]$Root = "reports/dhan_phase2_backfill_20210101_20260715",
    [string]$StatusDir = "reports/dhan_phase2_backfill_20210101_20260715/supervisor",
    [int]$PollSeconds = 10,
    [int]$StallSeconds = 180,
    [int]$MaxRestarts = 3,
    [int]$RestartBackoffSeconds = 30
)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
$Source = Join-Path $Repo "src"
$existingPythonPath = [Environment]::GetEnvironmentVariable("PYTHONPATH", "Process")
if ([string]::IsNullOrWhiteSpace($existingPythonPath)) {
    $env:PYTHONPATH = $Source
} else {
    $env:PYTHONPATH = "$Source;$existingPythonPath"
}

# Credentials are intentionally inherited from the process environment. Never add
# DHAN_ACCESS_TOKEN, DHAN_TOKEN, or any other credential to this argument list.
$arguments = @(
    "-m", "dhan_data_fetch_stream.supervisor",
    "--root", $Root,
    "--status-dir", $StatusDir,
    "--start-date", "2021-01-01",
    "--end-date", "2026-07-15",
    "--expiry-codes", "1",
    "--expiry-flags", "WEEK,MONTH",
    "--option-types", "CALL,PUT",
    "--moneyness-width", "10",
    "--workers", "5",
    "--requests-per-second", "5",
    "--daily-budget", "100000",
    "--max-retries", "4",
    "--timeout-seconds", "30",
    "--poll-seconds", $PollSeconds.ToString(),
    "--stall-seconds", $StallSeconds.ToString(),
    "--max-restarts", $MaxRestarts.ToString(),
    "--restart-backoff-seconds", $RestartBackoffSeconds.ToString(),
    "--expected-cells", "8820"
)

$resolvedStatus = if ([IO.Path]::IsPathRooted($StatusDir)) { $StatusDir } else { Join-Path $Repo $StatusDir }
New-Item -ItemType Directory -Path $resolvedStatus -Force | Out-Null
$stdout = Join-Path $resolvedStatus "supervisor_stdout.log"
$stderr = Join-Path $resolvedStatus "supervisor_stderr.log"
$python = (Get-Command "python" -ErrorAction Stop).Source
$quotedArguments = $arguments | ForEach-Object {
    $value = [string]$_
    if ($value -match '[\s"]') {
        '"' + $value.Replace('"', '\"') + '"'
    } else {
        $value
    }
}
$process = Start-Process `
    -FilePath $python `
    -ArgumentList $quotedArguments `
    -WorkingDirectory $Repo `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -WindowStyle Hidden `
    -PassThru

[PSCustomObject]@{
    supervisor_pid = $process.Id
    status_json = (Join-Path $resolvedStatus "status.json")
    status_markdown = (Join-Path $resolvedStatus "STATUS.md")
    event_log = (Join-Path $resolvedStatus "events.jsonl")
    credentials_in_command = $false
}
