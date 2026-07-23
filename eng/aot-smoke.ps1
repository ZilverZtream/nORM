<#
.SYNOPSIS
    Publishes eng/aot-smoke to a NativeAOT binary and runs its scenario matrix.

.DESCRIPTION
    Proves nORM's source-generated path (reads + direct/tracked writes) executes under NativeAOT.
    This is an opt-in verification asset, not part of the normal test run: a native publish needs the
    platform's AOT toolchain (a C/C++ compiler + linker: MSVC on Windows, clang/gcc on Linux/macOS)
    that CI runners don't all carry, and each publish takes minutes. Run it manually before releases
    or from a dedicated CI job.

    Exit code: 0 = all scenarios passed; non-zero = that many scenarios failed (or a publish error).

.EXAMPLE
    pwsh eng/aot-smoke.ps1
    pwsh eng/aot-smoke.ps1 -Rid linux-x64
#>
param(
    [string]$Rid = "",
    [string]$Configuration = "Release"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSCommandPath
$proj = Join-Path $root "aot-smoke/AotSmoke.csproj"

if (-not $Rid) {
    $rt = [System.Runtime.InteropServices.RuntimeInformation]
    $osp = [System.Runtime.InteropServices.OSPlatform]
    if ($rt::IsOSPlatform($osp::Windows) -or $env:OS -eq "Windows_NT") { $Rid = "win-x64" }
    elseif ($rt::IsOSPlatform($osp::OSX)) { $Rid = "osx-x64" }
    else { $Rid = "linux-x64" }
}

Write-Host "== nORM NativeAOT smoke ==" -ForegroundColor Cyan
Write-Host "project: $proj"
Write-Host "rid:     $Rid  configuration: $Configuration"

$pubDir = Join-Path $root "aot-smoke/bin/$Configuration/net8.0/$Rid/publish"
$exeName = if ($Rid -like "win-*") { "AotSmoke.exe" } else { "AotSmoke" }
$exe = Join-Path $pubDir $exeName

Write-Host "`n-- publishing (native compile; this takes a few minutes) --" -ForegroundColor Yellow
& dotnet publish $proj -c $Configuration -r $Rid --nologo -v minimal
if ($LASTEXITCODE -ne 0) {
    Write-Host "PUBLISH FAILED (exit $LASTEXITCODE). If this is a missing-toolchain error, install the platform AOT prerequisites (https://learn.microsoft.com/dotnet/core/deploying/native-aot/#prerequisites)." -ForegroundColor Red
    exit $LASTEXITCODE
}

if (-not (Test-Path $exe)) {
    Write-Host "PUBLISH SUCCEEDED but native binary not found at $exe" -ForegroundColor Red
    exit 1
}

$sizeMb = [math]::Round((Get-Item $exe).Length / 1MB, 2)
Write-Host "`n-- running native binary ($sizeMb MB) --" -ForegroundColor Yellow
& $exe
$code = $LASTEXITCODE

Write-Host ""
if ($code -eq 0) {
    Write-Host "AOT SMOKE PASSED -- nORM runs under NativeAOT." -ForegroundColor Green
} else {
    Write-Host "AOT SMOKE FAILED -- $code scenario(s) failed." -ForegroundColor Red
}
exit $code
