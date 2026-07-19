param(
    [string]$InputRoot = "reports/dhan_phase3_pre_bsm_quality_patch_20210101_20260715/enriched_options/version=2.1.0",
    [string]$OutputRoot = "reports/dhan_phase3_bsm_quality_patch_pilot_202101_202301_202601",
    [string[]]$Months = @("2021-01", "2023-01", "2026-01")
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$repo\src;$env:PYTHONPATH" } else { "$repo\src" }
$manifestRoot = Join-Path $repo "$OutputRoot\version=2.1.0\manifests"
New-Item -ItemType Directory -Path $manifestRoot -Force | Out-Null
$stdout = Join-Path $manifestRoot "patched_pilot.stdout.log"
$stderr = Join-Path $manifestRoot "patched_pilot.stderr.log"
$arguments = @(
    "-3.11", "-m", "dhan_data_fetch_stream.cli", "bsm-v2",
    "--input-root", $InputRoot,
    "--output-root", $OutputRoot,
    "--months", ($Months -join ","),
    "--row-group-size", "250000",
    "--max-newton-iterations", "20",
    "--max-brent-iterations", "100",
    "--json"
)
& py @arguments 1> $stdout 2> $stderr
exit $LASTEXITCODE
