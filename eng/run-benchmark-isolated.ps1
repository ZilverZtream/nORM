<#
.SYNOPSIS
    Runs the nORM benchmark project safely from a repository that contains agent
    worktrees (e.g. .claude/worktrees), passing arbitrary arguments through to
    `dotnet run` in the benchmarks project.

.DESCRIPTION
    Agent worktrees under .claude/worktrees are full checkouts of the repository,
    so each one contains its own benchmarks/nORM.Benchmarks.csproj. When more than
    one nORM.Benchmarks.csproj exists under the repository root, BenchmarkDotNet's
    project discovery becomes ambiguous and the run fails before any measurement.

    This script detects that situation and, when needed, runs the benchmark inside
    a clean detached `git worktree` created under the system temp directory. That
    temporary worktree is a checkout of HEAD only, so it never contains the
    .claude/worktrees copies. Raw BenchmarkDotNet reports are copied back into the
    canonical benchmarks/BenchmarkDotNet.Artifacts folder, and the temporary
    worktree is removed afterwards.

    The user's own worktrees under .claude/worktrees are never touched.

.PARAMETER Configuration
    Build configuration. Defaults to Release (the only configuration valid for
    published benchmark evidence).

.PARAMETER KeepWorktree
    Leave the temporary isolation worktree on disk for inspection instead of
    removing it.

.PARAMETER NoIsolation
    Skip duplicate detection and run directly in benchmarks/. Use only inside a
    checkout that is already known to be free of duplicate benchmark projects.

.PARAMETER BenchmarkArgs
    Everything after `--` is forwarded verbatim to `dotnet run -- ...`.

.EXAMPLE
    ./eng/run-benchmark-isolated.ps1 -- --filter "*TenantTemporalBenchmarks*"

.EXAMPLE
    ./eng/run-benchmark-isolated.ps1 -- --provider-matrix --provider Sqlite --filter "*ProviderMatrixBenchmarks.Insert_Single*"

.EXAMPLE
    ./eng/run-benchmark-isolated.ps1 -- --fast Query_Complex
#>
param(
    [string]$Configuration = 'Release',
    [switch]$KeepWorktree,
    [switch]$NoIsolation,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$BenchmarkArgs
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$benchmarkProjectDir = Join-Path $root 'benchmarks'
$benchmarkProjectPath = Join-Path $benchmarkProjectDir 'nORM.Benchmarks.csproj'

# PowerShell leaves a leading literal `--` in the remaining-args array; drop it
# so the forwarded command line matches what the user typed after the separator.
$forwarded = @($BenchmarkArgs | Where-Object { $_ -ne '--' })

function Get-DuplicateBenchmarkProjects {
    $expectedProject = [System.IO.Path]::GetFullPath($benchmarkProjectPath)
    return @(Get-ChildItem -LiteralPath $root -Recurse -File -Filter 'nORM.Benchmarks.csproj' |
        Where-Object { [System.IO.Path]::GetFullPath($_.FullName) -ne $expectedProject })
}

$workingBenchmarkProjectDir = $benchmarkProjectDir
$tempRoot = $null
$runStamp = Get-Date -Format 'yyyyMMdd-HHmmss'

if (-not $NoIsolation) {
    $duplicates = Get-DuplicateBenchmarkProjects
    if ($duplicates.Count -gt 0) {
        $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('norm-bench-iso-' + $runStamp)
        Write-Host "Duplicate benchmark projects detected under repo ($($duplicates.Count) extra). Running isolated worktree at $tempRoot"
        git -C $root worktree add --detach $tempRoot HEAD | Write-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create isolated benchmark worktree at $tempRoot."
        }
        $workingBenchmarkProjectDir = Join-Path $tempRoot 'benchmarks'
    }
}

$benchmarkExitCode = 0
try {
    Push-Location $workingBenchmarkProjectDir
    try {
        Write-Host "Running: dotnet run -c $Configuration -- $($forwarded -join ' ')"
        dotnet run -c $Configuration -- @forwarded
        $benchmarkExitCode = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } else { 0 }
    }
    finally {
        Pop-Location
    }

    if ($tempRoot) {
        $tempArtifacts = Join-Path $workingBenchmarkProjectDir 'BenchmarkDotNet.Artifacts'
        if (Test-Path $tempArtifacts) {
            $localArtifacts = Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts'
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $localArtifacts
            Copy-Item -LiteralPath $tempArtifacts -Destination $benchmarkProjectDir -Recurse -Force
            Write-Host "Copied benchmark reports back to $localArtifacts"
        }
    }
}
finally {
    if ($tempRoot -and -not $KeepWorktree) {
        git -C $root worktree remove --force $tempRoot | Write-Host
    }
    elseif ($tempRoot -and $KeepWorktree) {
        Write-Host "Isolation worktree kept at $tempRoot (remove with: git -C `"$root`" worktree remove --force `"$tempRoot`")"
    }
}

if ($benchmarkExitCode -ne 0) {
    throw "Benchmark command failed with exit code $benchmarkExitCode."
}
