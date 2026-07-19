param(
    [string]$OutputRoot = "reports/dhan_phase3_pre_bsm_v2_20210101_20260715",
    [string]$TempDirectory = "reports/dhan_phase3_pre_bsm_v2_20210101_20260715_spill",
    [int]$Threads = 8,
    [string]$MemoryLimit = "8GB"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$statusDir = Join-Path $repo "$OutputRoot/enriched_options/version=2.0.0/manifests"
New-Item -ItemType Directory -Path $statusDir -Force | Out-Null
$launcherStdout = Join-Path $statusDir "pre_bsm_v2_launcher_stdout.log"
$launcherStderr = Join-Path $statusDir "pre_bsm_v2_launcher_stderr.log"
$supervisorScript = Join-Path $PSScriptRoot "run_pre_bsm_v2_full.ps1"
$supervisorArgs = @(
    "-NoProfile", "-File", $supervisorScript,
    "-OutputRoot", $OutputRoot,
    "-TempDirectory", $TempDirectory,
    "-Threads", $Threads.ToString(),
    "-MemoryLimit", $MemoryLimit
)
$supervisor = Start-Process -FilePath "pwsh" -ArgumentList $supervisorArgs -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $launcherStdout -RedirectStandardError $launcherStderr

$keepAwakeScript = Join-Path $PSScriptRoot "keep_awake_while_pid.ps1"
$keepAwakeStdout = Join-Path $statusDir "pre_bsm_v2_keep_awake_stdout.log"
$keepAwakeStderr = Join-Path $statusDir "pre_bsm_v2_keep_awake_stderr.log"
$keepAwake = Start-Process -FilePath "pwsh" -ArgumentList @(
    "-NoProfile", "-File", $keepAwakeScript,
    "-ProcessId", $supervisor.Id.ToString(), "-PollSeconds", "30"
) -PassThru -WindowStyle Hidden -RedirectStandardOutput $keepAwakeStdout -RedirectStandardError $keepAwakeStderr

[pscustomobject]@{
    launched_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    supervisor_pid = $supervisor.Id
    keep_awake_pid = $keepAwake.Id
    command = "pwsh -NoProfile -File scripts/run_pre_bsm_v2_full.ps1 -OutputRoot $OutputRoot -TempDirectory $TempDirectory -Threads $Threads -MemoryLimit $MemoryLimit"
    output_root = (Join-Path $repo "$OutputRoot/enriched_options/version=2.0.0")
    status_path = (Join-Path $statusDir "pre_bsm_v2_status.json")
    supervisor_status_path = (Join-Path $statusDir "pre_bsm_v2_supervisor.json")
} | ConvertTo-Json -Depth 4
