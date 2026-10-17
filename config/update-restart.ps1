$ErrorActionPreference = "Stop"

try {
    $parentId = [int]$env:UPDATE_PARENT_PID
    $source = $env:UPDATE_SOURCE
    $destination = $env:UPDATE_EXECUTABLE
    $workingDirectory = $env:UPDATE_WORKING_DIR

    if ($parentId -le 0 -or $parentId -eq $PID) { throw "Invalid parent PID" }
    if (!(Test-Path -LiteralPath $source -PathType Leaf) -or (Get-Item -LiteralPath $source).Length -eq 0) {
        throw "Update file is missing or empty"
    }
    if (!(Test-Path -LiteralPath $destination -PathType Leaf) -or $source -eq $destination) {
        throw "Invalid destination executable"
    }
    if (!(Test-Path -LiteralPath $workingDirectory -PathType Container)) { throw "Invalid working directory" }
    $parentProcess = Get-Process -Id $parentId -ErrorAction Stop
    # Acquire the process handle before acknowledging readiness, avoiding PID reuse
    $null = $parentProcess.Handle

    [Console]::Out.WriteLine("UPDATE_READY")
    [Console]::Out.Flush()
    $parentProcess.WaitForExit()
    $parentProcess.Dispose()

    Move-Item -LiteralPath $source -Destination $destination -Force
#     Start-Process -FilePath $destination -WorkingDirectory $workingDirectory -WindowStyle Hidden `
#         -RedirectStandardOutput (Join-Path $workingDirectory "pool-update.stdout.log") `
#         -RedirectStandardError (Join-Path $workingDirectory "pool-update.stderr.log")
    Start-Process -FilePath "cmd.exe" -WorkingDirectory $workingDirectory `
        -ArgumentList "/k `"`"$destination`"`""
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
