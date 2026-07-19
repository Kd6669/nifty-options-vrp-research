param(
    [Parameter(Mandatory = $true)]
    [string]$RunRoot,
    [double]$MinimumFreeGB = 15.0,
    [int]$PollSeconds = 60
)

$ErrorActionPreference = "Stop"
$resolvedRunRoot = [System.IO.Path]::GetFullPath($RunRoot)
$driveRoot = [System.IO.Path]::GetPathRoot($resolvedRunRoot)
$logPath = Join-Path $resolvedRunRoot "logs\disk_guard.log"
$escapedRunRoot = [regex]::Escape($resolvedRunRoot)

while ($true) {
    $spanProcesses = @(
        Get-CimInstance Win32_Process | Where-Object {
            $_.Name -eq "python.exe" -and
            $_.CommandLine -match "span-backfill\s+download" -and
            $_.CommandLine -match $escapedRunRoot
        }
    )
    if ($spanProcesses.Count -eq 0) {
        Add-Content -LiteralPath $logPath -Value "$(Get-Date -Format o) downloader_not_running guard_exit"
        exit 0
    }

    $drive = [System.IO.DriveInfo]::new($driveRoot)
    $freeGB = $drive.AvailableFreeSpace / 1GB
    if ($freeGB -le $MinimumFreeGB) {
        $processIds = @($spanProcesses.ProcessId)
        $pids = @(
            $spanProcesses |
                Sort-Object @{ Expression = { if ($processIds -contains $_.ParentProcessId) { 0 } else { 1 } } } |
                ForEach-Object ProcessId
        )
        Add-Content -LiteralPath $logPath -Value (
            "$(Get-Date -Format o) low_disk_stop free_gb=$([math]::Round($freeGB, 3)) " +
            "minimum_free_gb=$MinimumFreeGB pids=$($pids -join ',')"
        )
        foreach ($processId in $pids) {
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        }
        exit 2
    }

    Add-Content -LiteralPath $logPath -Value (
        "$(Get-Date -Format o) healthy free_gb=$([math]::Round($freeGB, 3)) " +
        "minimum_free_gb=$MinimumFreeGB pids=$(@($spanProcesses.ProcessId) -join ',')"
    )
    Start-Sleep -Seconds $PollSeconds
}
