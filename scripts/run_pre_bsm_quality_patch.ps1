param(
    [string]$InputRoot = "reports/dhan_phase3_pre_bsm_v2_20210101_20260715/enriched_options/version=2.0.0",
    [string]$OutputRoot = "reports/dhan_phase3_pre_bsm_quality_patch_20210101_20260715",
    [string[]]$Months = @(),
    [int]$Threads = 8,
    [string]$MemoryLimit = "8GB"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$repo\src;$env:PYTHONPATH" } else { "$repo\src" }
$logRoot = Join-Path $repo "$OutputRoot\enriched_options\version=2.1.0\manifests"
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$stdout = Join-Path $logRoot "quality_patch_stdout.log"
$stderr = Join-Path $logRoot "quality_patch_stderr.log"
$arguments = @(
    "-3.11", "-m", "dhan_data_fetch_stream.pre_bsm_quality_patch",
    "--input-root", $InputRoot,
    "--output-root", $OutputRoot,
    "--threads", $Threads,
    "--memory-limit", $MemoryLimit,
    "--temp-directory", (Join-Path $repo "$OutputRoot\duckdb_tmp")
)
if ($Months.Count -gt 0) {
    $arguments += "--months"
    $arguments += @($Months | ForEach-Object { $_ -split ',' } | Where-Object { $_ })
}
& py @arguments 1>> $stdout 2>> $stderr
exit $LASTEXITCODE
