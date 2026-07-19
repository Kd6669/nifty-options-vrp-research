param(
    [Parameter(Mandatory = $true)]
    [string]$SpanRoot,
    [string[]]$Months = @(),
    [string]$BodOutputRoot = "reports\nifty_gold_span_bod_20210101_20260715",
    [string]$SixSlotOutputRoot = "reports\nifty_gold_span_six_slot_20210101_20260715",
    [int]$Threads = 8,
    [string]$MemoryLimit = "8GB"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$BsmRoot = Join-Path $RepoRoot "reports\dhan_phase3_bsm_quality_patch_20210101_20260715\version=2.1.0"
$arguments = @(
    "-3.11", "-m", "dhan_data_fetch_stream.cli", "span-release",
    "--bsm-root", $BsmRoot,
    "--bsm-terminal-audit", (Join-Path $BsmRoot "manifests\bsm_v2_terminal_audit.json"),
    "--span-compacted-root", (Join-Path $SpanRoot "compacted"),
    "--span-release-manifest", (Join-Path $SpanRoot "reports\final\SPAN_PHASE1_RELEASE_MANIFEST.json"),
    "--span-handoff", (Join-Path $SpanRoot "reports\final\DHAN_SPAN_HANDOFF.json"),
    "--span-source-gap-manifest", (Join-Path $SpanRoot "reports\final\span_source_gap_manifest.parquet"),
    "--bod-output-root", $BodOutputRoot,
    "--six-slot-output-root", $SixSlotOutputRoot,
    "--threads", $Threads,
    "--memory-limit", $MemoryLimit,
    "--row-group-size", "250000",
    "--json"
)
if ($Months.Count -gt 0) {
    $arguments += @("--months", ($Months -join ","))
}

Push-Location $RepoRoot
try {
    $env:PYTHONPATH = "src"
    & py @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "span-release failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
