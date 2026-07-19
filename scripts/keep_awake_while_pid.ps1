[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, [int]::MaxValue)]
    [int]$ProcessId,

    [ValidateRange(5, 300)]
    [int]$PollSeconds = 30
)

$ErrorActionPreference = "Stop"

Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class ExecutionState {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern uint SetThreadExecutionState(uint flags);
}
"@

$continuous = [Convert]::ToUInt32("80000000", 16)
$systemRequired = [uint32]0x00000001

try {
    while (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue) {
        $result = [ExecutionState]::SetThreadExecutionState($continuous -bor $systemRequired)
        if ($result -eq 0) {
            throw "SetThreadExecutionState failed"
        }
        Start-Sleep -Seconds $PollSeconds
    }
}
finally {
    [void][ExecutionState]::SetThreadExecutionState($continuous)
}
