[CmdletBinding()]
param(
    [string]$OutputRoot = "reports/dhan_phase3_pre_bsm_20210101_20260715"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = Join-Path $repo "src"
$statusDir = Join-Path $repo "$OutputRoot/enriched_options/version=1.0.0/manifests"
New-Item -ItemType Directory -Path $statusDir -Force | Out-Null

$arguments = @(
    "-3.11", "-m", "dhan_data_fetch_stream.cli", "pre-bsm-enrich",
    "--options-root", "reports/dhan_phase2_backfill_20210101_20260715/silver/options",
    "--spot-root", "reports/dhan_phase2_backfill_20210101_20260715/silver/spot",
    "--vix-root", "reports/dhan_phase2_vix_backfill_20210101_20260715/silver/india_vix",
    "--contract-rules", "docs/nse_rules/nse_contract_rule_dimension.json",
    "--actual-expiries", "docs/nse_rules/dhan_expiry_code_1_mapping.json",
    "--output-root", $OutputRoot,
    "--acquisition-terminally-accounted",
    "--json"
)

$stdout = Join-Path $statusDir "pre_bsm_stdout.log"
$stderr = Join-Path $statusDir "pre_bsm_stderr.log"
$python = (Get-Command "py" -ErrorAction Stop).Source

Push-Location $repo
try {
    & $python @arguments 1>> $stdout 2>> $stderr
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
