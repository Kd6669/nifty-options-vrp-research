param(
    [Parameter(Mandatory = $true)]
    [string]$WaitForPids,
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$RunRoot,
    [string]$StartDate = "2021-01-01",
    [string]$EndDate = "2026-07-15",
    [int]$DownloadConcurrency = 1,
    [int]$QueueSize = 2,
    [int]$MaxAttempts = 4,
    [int]$RetryIncompletePasses = 2,
    [double]$TimeoutSeconds = 600,
    [int]$SessionRefreshRequests = 20,
    [double]$MinimumFreeGB = 15.0
)

$ErrorActionPreference = "Stop"
$repo = [System.IO.Path]::GetFullPath($RepoRoot)
$run = [System.IO.Path]::GetFullPath($RunRoot)
$rawRoot = Join-Path $run "raw"
$manifest = Join-Path $run "manifests\download.jsonl"
$logRoot = Join-Path $run "logs"
$supervisorLog = Join-Path $logRoot "download.repair1.supervisor.log"
$stdoutLog = Join-Path $logRoot "download.repair1.stdout.log"
$stderrLog = Join-Path $logRoot "download.repair1.stderr.log"
$python = Join-Path $repo ".venv\Scripts\python.exe"
$guardScript = Join-Path $repo "scripts\monitor_span_backfill_disk.ps1"
$waitPids = @(
    $WaitForPids.Split(",", [System.StringSplitOptions]::RemoveEmptyEntries) |
        ForEach-Object { [int]$_.Trim() }
)
if ($waitPids.Count -eq 0) {
    throw "WaitForPids must contain at least one process id"
}

New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

function Write-SupervisorLog {
    param([string]$Message)
    Add-Content -LiteralPath $supervisorLog -Value "$(Get-Date -Format o) $Message"
}

Write-SupervisorLog "waiting_for_exact_pids=$($waitPids -join ',')"
foreach ($processId in $waitPids) {
    $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if ($null -ne $process) {
        Wait-Process -Id $processId
    }
}
Write-SupervisorLog "wait_boundary_reached"

$escapedManifest = [regex]::Escape($manifest)
$otherWriters = @(
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "python.exe" -and
        $_.CommandLine -match "span-backfill\s+download" -and
        $_.CommandLine -match $escapedManifest
    }
)
if ($otherWriters.Count -gt 0) {
    Write-SupervisorLog "abort_other_manifest_writer_pids=$(@($otherWriters.ProcessId) -join ',')"
    exit 3
}

if (-not (Test-Path -LiteralPath $python)) {
    Write-SupervisorLog "abort_python_missing=$python"
    exit 4
}
if (-not (Test-Path -LiteralPath $manifest)) {
    Write-SupervisorLog "abort_manifest_missing=$manifest"
    exit 5
}

$arguments = @(
    "-u", "-m", "robs_live.cli", "span-backfill", "download",
    "--start-date", $StartDate,
    "--end-date", $EndDate,
    "--raw-root", $rawRoot,
    "--download-manifest", $manifest,
    "--download-concurrency", $DownloadConcurrency,
    "--queue-size", $QueueSize,
    "--max-attempts", $MaxAttempts,
    "--retry-incomplete-passes", $RetryIncompletePasses,
    "--timeout-seconds", $TimeoutSeconds,
    "--session-refresh-requests", $SessionRefreshRequests,
    "--json"
)

$repair = Start-Process `
    -FilePath $python `
    -ArgumentList $arguments `
    -WorkingDirectory $repo `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden `
    -PassThru
Write-SupervisorLog (
    "repair_launched_pid=$($repair.Id) concurrency=$DownloadConcurrency queue_size=$QueueSize " +
    "max_attempts=$MaxAttempts retry_incomplete_passes=$RetryIncompletePasses " +
    "timeout_seconds=$TimeoutSeconds stdout=$stdoutLog stderr=$stderrLog"
)

Start-Sleep -Seconds 2
$escapedRunRoot = [regex]::Escape($run)
$guards = @(
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -in @("pwsh.exe", "powershell.exe") -and
        $_.CommandLine -match "monitor_span_backfill_disk\.ps1" -and
        $_.CommandLine -match $escapedRunRoot
    }
)
if ($guards.Count -eq 0) {
    $guard = Start-Process `
        -FilePath "pwsh.exe" `
        -ArgumentList @(
            "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", $guardScript,
            "-RunRoot", $run,
            "-MinimumFreeGB", $MinimumFreeGB,
            "-PollSeconds", 60
        ) `
        -WorkingDirectory $repo `
        -WindowStyle Hidden `
        -PassThru
    Write-SupervisorLog "disk_guard_relaunched_pid=$($guard.Id) minimum_free_gb=$MinimumFreeGB"
} else {
    Write-SupervisorLog "disk_guard_already_running_pids=$(@($guards.ProcessId) -join ',')"
}
