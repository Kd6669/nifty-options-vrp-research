param(
    [string]$Python = "python",
    [string]$Archive = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    $arguments = @("-m", "tools.team_bundle", "build")
    if ($Archive) {
        $arguments += @("--archive", $Archive)
    }
    & $Python @arguments
    if ($LASTEXITCODE -ne 0) { throw "Team bundle build failed." }

    $verifyArguments = @("-m", "tools.team_bundle", "verify")
    if ($Archive) {
        $verifyArguments += @("--archive", $Archive)
    }
    & $Python @verifyArguments
    if ($LASTEXITCODE -ne 0) { throw "Team bundle verification failed." }
}
finally {
    Pop-Location
}
