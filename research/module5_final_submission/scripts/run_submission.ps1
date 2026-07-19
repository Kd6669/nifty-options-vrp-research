param(
    [switch]$SkipTests,
    [switch]$SkipBinaryRebuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
Push-Location $RepoRoot
try {
    python -m research.module5_final_submission.run build
    python -m research.module5_final_submission.run verify
    if (-not $SkipBinaryRebuild) {
        python research\module5_final_submission\scripts\build_pdf.py
        $Node = Get-Command node -ErrorAction SilentlyContinue
        $ArtifactTool = Test-Path (Join-Path $RepoRoot "node_modules\@oai\artifact-tool")
        if ($Node -and $ArtifactTool) {
            & $Node.Source research\module5_final_submission\scripts\build_workbook.mjs
            & $Node.Source research\module5_final_submission\scripts\verify_workbook.mjs
        }
        elseif (-not (Test-Path "submission\NIFTY_VRP_Research_Tearsheet.xlsx")) {
            throw "Workbook is absent and @oai/artifact-tool is unavailable. Install the workbook runtime or restore the versioned artifact."
        }
        else {
            Write-Warning "@oai/artifact-tool is unavailable; retaining the hash-verified versioned workbook."
        }
    }
    python research\module5_final_submission\scripts\build_submission_manifest.py
    if (-not $SkipTests) {
        python -m pytest -q
    }
}
finally {
    Pop-Location
}
