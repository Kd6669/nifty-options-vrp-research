param(
    [string]$Python = "python",
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    & $Python tools/audit_sample.py samples/nifty_gold_sample.parquet samples/nifty_gold_sample.manifest.json
    if ($LASTEXITCODE -ne 0) { throw "Sample audit failed." }

    foreach ($module in @(
        "research.module3_hypothesis_testing.run",
        "research.module4_sizing_risk_management.run",
        "research.module5_final_submission.run"
    )) {
        & $Python -m $module build
        if ($LASTEXITCODE -ne 0) { throw "$module build failed." }
        & $Python -m $module verify
        if ($LASTEXITCODE -ne 0) { throw "$module verification failed." }
    }

    if (-not $SkipTests) {
        & $Python -m pytest -q
        if ($LASTEXITCODE -ne 0) { throw "Test suite failed." }
    }
}
finally {
    Pop-Location
}
