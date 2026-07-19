param(
    [string]$InputRoot = "reports/dhan_phase3_pre_bsm_quality_patch_20210101_20260715/enriched_options/version=2.1.0",
    [string]$OutputRoot = "reports/dhan_phase3_bsm_quality_patch_20210101_20260715"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$supervisorScript = Join-Path $PSScriptRoot "run_bsm_v2_full.ps1"
$keepAwakeScript = Join-Path $PSScriptRoot "keep_awake_while_pid.ps1"
$supervisorCommand = "pwsh -NoProfile -File `"$supervisorScript`" -InputRoot `"$InputRoot`" -OutputRoot `"$OutputRoot`""

# Win32_Process.Create is intentionally used here. The WMI service becomes the
# parent, so the durable supervisor is not tied to the short-lived Codex shell
# job that invoked this launcher.
$supervisor = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = $supervisorCommand
    CurrentDirectory = $repo
}
if ($supervisor.ReturnValue -ne 0 -or $supervisor.ProcessId -le 0) {
    throw "Failed to create detached BSM supervisor: return=$($supervisor.ReturnValue)"
}
$keepAwakeCommand = "pwsh -NoProfile -File `"$keepAwakeScript`" -ProcessId $($supervisor.ProcessId) -PollSeconds 30"
$keepAwake = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = $keepAwakeCommand
    CurrentDirectory = $repo
}
if ($keepAwake.ReturnValue -ne 0 -or $keepAwake.ProcessId -le 0) {
    Stop-Process -Id $supervisor.ProcessId -ErrorAction SilentlyContinue
    throw "Failed to create detached keep-awake helper: return=$($keepAwake.ReturnValue)"
}

[pscustomobject]@{
    launched_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    supervisor_pid = $supervisor.ProcessId
    keep_awake_pid = $keepAwake.ProcessId
    detached_parent = "Win32_Process.Create"
    command = $supervisorCommand
    status_path = (Join-Path $repo "$OutputRoot/version=2.1.0/manifests/bsm_v2_supervisor.json")
} | ConvertTo-Json -Depth 4
