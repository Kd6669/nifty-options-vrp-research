param(
    [string]$InputRoot = "reports/dhan_phase3_pre_bsm_quality_patch_20210101_20260715/enriched_options/version=2.1.0",
    [string]$OutputRoot = "reports/dhan_phase3_bsm_quality_patch_20210101_20260715",
    [int]$MaxRestartsPerMonth = 2,
    [int]$RestartBackoffSeconds = 30
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$repo\src;$env:PYTHONPATH" } else { "$repo\src" }
$versionRoot = Join-Path $repo "$OutputRoot/version=2.1.0"
$statusDir = Join-Path $versionRoot "manifests"
New-Item -ItemType Directory -Path $statusDir -Force | Out-Null
$statusPath = Join-Path $statusDir "bsm_v2_supervisor.json"
$events = Join-Path $statusDir "bsm_v2_supervisor.events.jsonl"
$stdout = Join-Path $statusDir "bsm_v2_stdout.log"
$stderr = Join-Path $statusDir "bsm_v2_stderr.log"
$startedAt = (Get-Date).ToUniversalTime().ToString("o")
$display = "py -3.11 -m dhan_data_fetch_stream.cli bsm-v2 --input-root $InputRoot --output-root $OutputRoot --months <YYYY-MM> --row-group-size 250000 --max-newton-iterations 20 --max-brent-iterations 100 --json"
$months = @()
$cursor = [datetime]::new(2021, 1, 1)
$end = [datetime]::new(2026, 7, 1)
while ($cursor -le $end) {
    $months += $cursor.ToString("yyyy-MM")
    $cursor = $cursor.AddMonths(1)
}

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

function Get-AggregateProgress {
    $rows = [int64]0
    $manifestCount = 0
    $metrics = @{
        ready_input_rows=[int64]0; blocked_input_rows=[int64]0; eligible_rows=[int64]0
        converged_rows=[int64]0; fallback_rows=[int64]0; no_arbitrage_rejects=[int64]0
        quality_severe_input_rows=[int64]0; quality_severe_solved_rows=[int64]0
        blocked_rows_with_finite_bsm_values=[int64]0
    }
    $statusCounts = @{}
    $methodCounts = @{}
    Get-ChildItem -LiteralPath $statusDir -Recurse -File -Filter "month=*.json" -ErrorAction SilentlyContinue | ForEach-Object {
        $manifest = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
        $rows += [int64]$manifest.output_rows
        $manifestCount++
        foreach($key in @($metrics.Keys)) { $metrics[$key] += [int64]$manifest.$key }
        foreach($property in $manifest.status_counts.PSObject.Properties) {
            if(-not $statusCounts.ContainsKey($property.Name)) { $statusCounts[$property.Name] = [int64]0 }
            $statusCounts[$property.Name] += [int64]$property.Value
        }
        foreach($property in $manifest.solver_method_counts.PSObject.Properties) {
            if(-not $methodCounts.ContainsKey($property.Name)) { $methodCounts[$property.Name] = [int64]0 }
            $methodCounts[$property.Name] += [int64]$property.Value
        }
    }
    return @{months_completed=$manifestCount; rows_completed=$rows; metrics=$metrics; status_counts=$statusCounts; solver_method_counts=$methodCounts}
}

for ($monthIndex = 0; $monthIndex -lt $months.Count; $monthIndex++) {
    $month = $months[$monthIndex]
    $attempt = 0
    while ($true) {
        $attempt++
        $arguments = @(
            "-3.11", "-m", "dhan_data_fetch_stream.cli", "bsm-v2",
            "--input-root", $InputRoot, "--output-root", $OutputRoot,
            "--months", $month, "--row-group-size", "250000",
            "--max-newton-iterations", "20", "--max-brent-iterations", "100", "--json"
        )
        $child = Start-Process -FilePath "py" -ArgumentList $arguments -PassThru -WindowStyle Hidden `
            -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        $progress = Get-AggregateProgress
        $status = @{
            status_version = "2.1.0"; state = "running"; isolation = "one_python_process_per_month"
            started_at_utc = $startedAt; updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
            supervisor_pid = $PID; child_pid = $child.Id; current_month = $month
            month_position = ($monthIndex + 1); months_total = $months.Count
            completed_month_manifests = $progress.months_completed; rows_completed = $progress.rows_completed
            solver_metrics = $progress.metrics; status_counts = $progress.status_counts
            solver_method_counts = $progress.solver_method_counts
            attempt_for_current_month = $attempt; max_restarts_per_month = $MaxRestartsPerMonth
            command = $display; output_root = $versionRoot; stderr_path = $stderr
        }
        Write-AtomicJson $statusPath $status
        Write-Event (@{event="month_child_started"; timestamp_utc=$status.updated_at_utc; month=$month; child_pid=$child.Id; attempt=$attempt})
        $child.WaitForExit()
        $exitCode = $child.ExitCode
        if ($exitCode -eq 0) {
            $progress = Get-AggregateProgress
            $status.completed_month_manifests = $progress.months_completed
            $status.rows_completed = $progress.rows_completed
            $status.solver_metrics = $progress.metrics
            $status.status_counts = $progress.status_counts
            $status.solver_method_counts = $progress.solver_method_counts
            $status.updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
            Write-AtomicJson $statusPath $status
            Write-Event (@{event="month_complete"; timestamp_utc=$status.updated_at_utc; month=$month; child_pid=$child.Id; exit_code=0})
            break
        }
        $stderrText = if (Test-Path $stderr) { Get-Content -LiteralPath $stderr -Raw } else { "" }
        $integrityPattern = "acceptance failed|row conservation|hash mismatch|primary.key|missing|required|invalid|unexpectedly already contains"
        $suppress = $stderrText -match $integrityPattern
        if ($suppress -or $attempt -ge ($MaxRestartsPerMonth + 1)) {
            $status.state = "blocked"; $status.exit_code = $exitCode; $status.restart_suppressed = $suppress
            $status.updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
            Write-AtomicJson $statusPath $status
            Write-Event (@{event="month_restart_suppressed"; timestamp_utc=$status.updated_at_utc; month=$month; exit_code=$exitCode; integrity_failure=$suppress})
            exit $exitCode
        }
        $backoff = $RestartBackoffSeconds * $attempt
        Write-Event (@{event="month_restart_scheduled"; timestamp_utc=(Get-Date).ToUniversalTime().ToString("o"); month=$month; exit_code=$exitCode; backoff_seconds=$backoff})
        Start-Sleep -Seconds $backoff
    }
}

$progress = Get-AggregateProgress
$complete = @{
    status_version = "2.1.0"; state = "complete"; isolation = "one_python_process_per_month"
    started_at_utc = $startedAt; updated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    supervisor_pid = $PID; child_pid = $null; current_month = $months[-1]
    month_position = $months.Count; months_total = $months.Count
    completed_month_manifests = $progress.months_completed; rows_completed = $progress.rows_completed
    solver_metrics = $progress.metrics; status_counts = $progress.status_counts
    solver_method_counts = $progress.solver_method_counts
    command = $display; output_root = $versionRoot; stderr_path = $stderr
}
Write-AtomicJson $statusPath $complete
Write-Event (@{event="supervisor_complete"; timestamp_utc=$complete.updated_at_utc; months=$progress.months_completed; rows=$progress.rows_completed})
exit 0
