param(
    [Parameter(Mandatory = $true)]
    [string]$SpanRoot,
    [string]$BsmRoot = "",
    [string]$OutputRoot = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not $BsmRoot) {
    $BsmRoot = Join-Path $RepoRoot "reports\dhan_phase3_bsm_quality_patch_20210101_20260715\version=2.1.0"
}
if (-not $OutputRoot) {
    $OutputRoot = Join-Path $RepoRoot "reports\nifty_gold_span_bod_20210101_20260715"
}

Push-Location $RepoRoot
try {
    $env:PYTHONPATH = "src"
    py -3.11 -m dhan_data_fetch_stream.cli span-gold `
        --bsm-root $BsmRoot `
        --bsm-terminal-audit (Join-Path $BsmRoot "manifests\bsm_v2_terminal_audit.json") `
        --span-compacted-root (Join-Path $SpanRoot "compacted") `
        --span-completion (Join-Path $SpanRoot "reports\final\SPAN_PHASE1_COMPLETION.json") `
        --span-matrix (Join-Path $SpanRoot "reports\final\audit\span_date_slot_matrix.parquet") `
        --output-root $OutputRoot `
        --threads 8 `
        --memory-limit 8GB `
        --row-group-size 250000 `
        --json
    if ($LASTEXITCODE -ne 0) {
        throw "span-gold failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
