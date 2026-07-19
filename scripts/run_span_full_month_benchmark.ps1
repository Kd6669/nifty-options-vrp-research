param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$RawFixtureRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [string]$PythonExe = "python.exe",
    [int]$Workers = 1,
    [int]$FreshRuns = 3,
    [int]$RerunRuns = 1
)

$ErrorActionPreference = "Stop"
$repo = [System.IO.Path]::GetFullPath($RepoRoot)
$raw = [System.IO.Path]::GetFullPath($RawFixtureRoot)
$output = [System.IO.Path]::GetFullPath($OutputRoot)
$statusLog = Join-Path $output "benchmark.status.log"
$benchmarkScript = Join-Path $repo "scripts\benchmark_span_extraction.py"
$compareScript = Join-Path $repo "scripts\compare_span_benchmarks.py"
$workersTag = "workers$Workers"
$legacyJson = Join-Path $output "span_legacy_full_month_$workersTag.json"
$streamingJson = Join-Path $output "span_streaming_full_month_$workersTag.json"
$comparisonJson = Join-Path $output "span_comparison_full_month_$workersTag.json"
$comparisonMarkdown = Join-Path $output "SPAN_FULL_MONTH_BENCHMARK.md"

if ($Workers -lt 1 -or $FreshRuns -lt 3 -or $RerunRuns -lt 1) {
    throw "Workers must be >=1, FreshRuns >=3, and RerunRuns >=1"
}
if (-not (Test-Path -LiteralPath $benchmarkScript)) {
    throw "benchmark script is missing: $benchmarkScript"
}
if (-not (Test-Path -LiteralPath $compareScript)) {
    throw "comparison script is missing: $compareScript"
}
$archives = @(Get-ChildItem -LiteralPath $raw -Filter "*.zip" -File -Recurse)
if ($archives.Count -lt 1) {
    throw "raw fixture contains no ZIP archives: $raw"
}

New-Item -ItemType Directory -Force -Path $output | Out-Null
$env:PYTHONPATH = Join-Path $repo "src"

function Write-Status {
    param([string]$Message)
    Add-Content -LiteralPath $statusLog -Value "$(Get-Date -Format o) $Message"
}

function Run-Benchmark {
    param(
        [string]$Implementation,
        [string]$EvidencePath
    )
    $stdout = Join-Path $output "$Implementation.stdout.log"
    $stderr = Join-Path $output "$Implementation.stderr.log"
    Write-Status "start implementation=$Implementation archives=$($archives.Count) workers=$Workers fresh_runs=$FreshRuns rerun_runs=$RerunRuns"
    & $PythonExe $benchmarkScript `
        --raw-dir $raw `
        --implementation $Implementation `
        --symbols NIFTY `
        --workers $Workers `
        --warmup-runs 0 `
        --fresh-runs $FreshRuns `
        --rerun-runs $RerunRuns `
        --output-json $EvidencePath `
        1> $stdout 2> $stderr
    $exitCode = $LASTEXITCODE
    Write-Status "finish implementation=$Implementation exit_code=$exitCode evidence=$EvidencePath stdout=$stdout stderr=$stderr"
    if ($exitCode -ne 0) {
        throw "$Implementation benchmark failed with exit code $exitCode"
    }
}

Write-Status "run_start raw=$raw output=$output python=$PythonExe"
Run-Benchmark -Implementation "legacy" -EvidencePath $legacyJson
Run-Benchmark -Implementation "streaming" -EvidencePath $streamingJson

$compareStdout = Join-Path $output "comparison.stdout.log"
$compareStderr = Join-Path $output "comparison.stderr.log"
& $PythonExe $compareScript `
    --legacy-json $legacyJson `
    --optimized-json $streamingJson `
    --output-json $comparisonJson `
    --output-markdown $comparisonMarkdown `
    --minimum-fresh-speedup 3 `
    1> $compareStdout 2> $compareStderr
$compareExit = $LASTEXITCODE
Write-Status "comparison_finish exit_code=$compareExit json=$comparisonJson markdown=$comparisonMarkdown"
if ($compareExit -ne 0) {
    throw "benchmark comparison failed with exit code $compareExit"
}
Write-Status "run_complete"
