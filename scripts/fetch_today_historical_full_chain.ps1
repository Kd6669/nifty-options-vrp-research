param(
    [Parameter(Mandatory = $true)]
    [string]$Expiry,
    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [string]$OutDir = "",
    [string]$Python = "py"
)

$ErrorActionPreference = "Stop"

if (-not $env:DHAN_ACCESS_TOKEN) {
    throw "DHAN_ACCESS_TOKEN must be set in the environment."
}

if (-not $OutDir) {
    $OutDir = "reports\dhan_intraday_1m_${Date}_full_chain"
}

& $Python -m dhan_data_fetch_stream.cli fetch-historical-full-chain `
    --date $Date `
    --expiry $Expiry `
    --out-dir $OutDir `
    --json
