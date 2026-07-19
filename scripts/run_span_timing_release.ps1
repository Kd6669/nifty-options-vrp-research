param(
    [string[]]$Months = @(),
    [string]$BaseSixSlotRoot = "reports\nifty_gold_span_six_slot_20210101_20260715\version=2.0.0",
    [string]$StrictOutputRoot = "reports\nifty_gold_span_point_in_time_strict_20210101_20260715",
    [string]$ResearchOutputRoot = "reports\nifty_gold_span_six_slot_research_20210101_20260715",
    [string]$OfficialTimingDocument = "reports\span_timing_sources\NCL_FAQ_RISK_MANAGEMENT_2025-08.pdf",
    [string]$FirstSeenManifest = "",
    [int]$Threads = 8,
    [string]$MemoryLimit = "8GB"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$arguments = @(
    "-3.11", "-m", "dhan_data_fetch_stream.cli", "span-timing-release",
    "--base-six-slot-root", $BaseSixSlotRoot,
    "--strict-output-root", $StrictOutputRoot,
    "--research-output-root", $ResearchOutputRoot,
    "--official-timing-document", $OfficialTimingDocument,
    "--threads", $Threads,
    "--memory-limit", $MemoryLimit,
    "--row-group-size", "250000",
    "--json"
)
if ($FirstSeenManifest) {
    $arguments += @("--first-seen-manifest", $FirstSeenManifest)
}
if ($Months.Count -gt 0) {
    $arguments += @("--months", ($Months -join ","))
}

Push-Location $RepoRoot
try {
    $env:PYTHONPATH = "src"
    & py @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "span-timing-release failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
