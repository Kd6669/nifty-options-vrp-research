param(
    [string]$OutputRoot = "reports/dhan_phase3_pre_bsm_v2_20210101_20260715",
    [string]$TempDirectory = "reports/dhan_phase3_pre_bsm_v2_20210101_20260715_spill",
    [int]$Threads = 8,
    [string]$MemoryLimit = "8GB",
    [int]$MaxRestarts = 2,
    [int]$RestartBackoffSeconds = 30
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$repo\src;$env:PYTHONPATH" } else { "$repo\src" }

$versionRoot = Join-Path $repo "$OutputRoot/enriched_options/version=2.0.0"
$statusDir = Join-Path $versionRoot "manifests"
New-Item -ItemType Directory -Path $statusDir -Force | Out-Null
$supervisorStatus = Join-Path $statusDir "pre_bsm_v2_supervisor.json"
$events = Join-Path $statusDir "pre_bsm_v2_supervisor.events.jsonl"
$childStdout = Join-Path $statusDir "pre_bsm_v2_stdout.log"
$childStderr = Join-Path $statusDir "pre_bsm_v2_stderr.log"

$arguments = @(
    "-3.11", "-m", "dhan_data_fetch_stream.cli", "pre-bsm-enrich-v2",
    "--options-root", "reports/dhan_phase2_backfill_20210101_20260715/silver/options",
    "--spot-root", "reports/dhan_phase2_backfill_20210101_20260715/silver/spot",
    "--vix-root", "reports/dhan_phase2_vix_backfill_20210101_20260715/silver/india_vix",
    "--contract-rules", "docs/nse_rules/nse_contract_rule_dimension.parquet",
    "--actual-expiries", "docs/nse_rules/dhan_expiry_code_1_mapping.parquet",
    "--output-root", $OutputRoot,
    "--temp-directory", $TempDirectory,
    "--threads", $Threads.ToString(),
    "--memory-limit", $MemoryLimit,
    "--row-group-size", "250000",
    "--acquisition-terminally-accounted",
    "--json"
)
$displayCommand = "py -3.11 -m dhan_data_fetch_stream.cli pre-bsm-enrich-v2 --options-root <immutable-options> --spot-root <immutable-spot> --vix-root <immutable-vix> --contract-rules <audited-rules> --actual-expiries <audited-expiries> --output-root $OutputRoot --temp-directory $TempDirectory --threads $Threads --memory-limit $MemoryLimit --row-group-size 250000 --acquisition-terminally-accounted --json"
$startedAt = (Get-Date).ToUniversalTime().ToString("o")

function Write-AtomicJson([string]$Path, [hashtable]$Payload) {
    $partial = "$Path.$([guid]::NewGuid().ToString('N')).partial"
    try {
        $Payload | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $partial -Encoding utf8
        Move-Item -LiteralPath $partial -Destination $Path -Force
    }
    finally {
        Remove-Item -LiteralPath $partial -Force -ErrorAction SilentlyContinue
    }
}

function Write-Event([hashtable]$Payload) {
    $Payload | ConvertTo-Json -Compress -Depth 8 | Add-Content -LiteralPath $events -Encoding utf8
}

$attempt = 0
while ($true) {
    $attempt++
    $child = Start-Process -FilePath "py" -ArgumentList $arguments -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $childStdout -RedirectStandardError $childStderr
    $running = @{
        status_version = "2.0.0"
        state = "running"
        started_at_utc = $startedAt
        updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        supervisor_pid = $PID
        child_pid = $child.Id
        attempt = $attempt
        max_restarts = $MaxRestarts
        command = $displayCommand
        output_root = $versionRoot
        runner_status = (Join-Path $statusDir "pre_bsm_v2_status.json")
        stderr_path = $childStderr
    }
    Write-AtomicJson $supervisorStatus $running
    Write-Event (@{event="child_started"; timestamp_utc=$running.updated_at_utc; supervisor_pid=$PID; child_pid=$child.Id; attempt=$attempt})
    $child.WaitForExit()
    $exitCode = $child.ExitCode
    if ($exitCode -eq 0) {
        $running.state = "complete"
        $running.updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        $running.exit_code = 0
        Write-AtomicJson $supervisorStatus $running
        Write-Event (@{event="child_complete"; timestamp_utc=$running.updated_at_utc; child_pid=$child.Id; exit_code=0})
        exit 0
    }

    $stderrText = if (Test-Path $childStderr) { Get-Content -LiteralPath $childStderr -Raw } else { "" }
    $integrityPattern = "row conservation|join multiplication|point-in-time join violation|Parquet metadata row mismatch|canonical/source-exception|primary.key|schema|missing required|invalid month|no partitioned option"
    $suppressRestart = $stderrText -match $integrityPattern
    if ($suppressRestart -or $attempt -ge ($MaxRestarts + 1)) {
        $running.state = "blocked"
        $running.updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        $running.exit_code = $exitCode
        $running.restart_suppressed = $suppressRestart
        Write-AtomicJson $supervisorStatus $running
        Write-Event (@{event="restart_suppressed"; timestamp_utc=$running.updated_at_utc; child_pid=$child.Id; exit_code=$exitCode; integrity_failure=$suppressRestart})
        exit $exitCode
    }

    $backoff = $RestartBackoffSeconds * $attempt
    Write-Event (@{event="restart_scheduled"; timestamp_utc=(Get-Date).ToUniversalTime().ToString("o"); exit_code=$exitCode; backoff_seconds=$backoff; next_attempt=($attempt + 1)})
    Start-Sleep -Seconds $backoff
}
