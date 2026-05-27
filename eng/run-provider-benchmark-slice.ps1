param(
    [string]$Providers = 'Sqlite,SqlServer,Postgres,MySql',
    [string]$Filters = '*ProviderMatrixBenchmarks.Query_Join*',
    [string]$Configuration = 'Release',
    [string]$OutputRoot = (Join-Path (Split-Path -Parent $PSScriptRoot) 'BenchmarkDotNet.Artifacts/provider-slices'),
    [switch]$CheckThresholds,
    [switch]$NoIsolation,
    [switch]$KeepWorktree
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$benchmarksDir = Join-Path $root 'benchmarks'
$runStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runRoot = Join-Path $OutputRoot $runStamp
$mergedResults = Join-Path $runRoot 'results'
$providerList = @($Providers -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$filterList = @($Filters -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ })

function Get-SafeName {
    param([string]$Value)
    return ($Value -replace '[^A-Za-z0-9_.-]', '_').Trim('_')
}

function Normalize-ProviderName {
    param([string]$Provider)

    switch ($Provider.ToLowerInvariant()) {
        'sqlite' { return 'Sqlite' }
        'sqlserver' { return 'SqlServer' }
        'sql-server' { return 'SqlServer' }
        'mssql' { return 'SqlServer' }
        'postgres' { return 'Postgres' }
        'postgresql' { return 'Postgres' }
        'mysql' { return 'MySql' }
        default { throw "Unsupported provider '$Provider'. Use Sqlite, SqlServer, Postgres, or MySql." }
    }
}

if (-not $NoIsolation) {
    $expectedProject = [System.IO.Path]::GetFullPath((Join-Path $benchmarksDir 'nORM.Benchmarks.csproj'))
    $benchmarkProjects = @(Get-ChildItem -LiteralPath $root -Recurse -File -Filter 'nORM.Benchmarks.csproj' |
        Where-Object { [System.IO.Path]::GetFullPath($_.FullName) -ne $expectedProject })

    if ($benchmarkProjects.Count -gt 0) {
        $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('norm-bench-slice-' + $runStamp)
        Write-Host "Duplicate benchmark projects detected under repo; running isolated worktree at $tempRoot"
        git -C $root worktree add --detach $tempRoot HEAD | Write-Host

        $forwardArgs = @(
            '-Providers', ($providerList -join ','),
            '-Filters', ($filterList -join ';'),
            '-Configuration', $Configuration,
            '-OutputRoot', $OutputRoot,
            '-NoIsolation'
        )
        if ($CheckThresholds) {
            $forwardArgs += '-CheckThresholds'
        }
        if ($KeepWorktree) {
            $forwardArgs += '-KeepWorktree'
        }

        try {
            & (Join-Path $tempRoot 'eng/run-provider-benchmark-slice.ps1') @forwardArgs
            $childExitCode = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
        }
        finally {
            if (-not $KeepWorktree) {
                git -C $root worktree remove --force $tempRoot | Write-Host
            }
        }

        exit $childExitCode
    }
}

New-Item -ItemType Directory -Force -Path $mergedResults | Out-Null

foreach ($providerArg in $providerList) {
    $provider = Normalize-ProviderName $providerArg

    foreach ($filter in $filterList) {
        $safeFilter = Get-SafeName $filter
        $sliceDir = Join-Path (Join-Path $runRoot $provider) $safeFilter
        $sliceResults = Join-Path $sliceDir 'results'
        New-Item -ItemType Directory -Force -Path $sliceResults | Out-Null

        Push-Location $benchmarksDir
        try {
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue '.\BenchmarkDotNet.Artifacts'
            $logPath = Join-Path $sliceDir 'benchmark.log'
            Write-Host "Running provider slice: provider=$provider filter=$filter"
            dotnet run -c $Configuration -- --provider-matrix --provider $provider --filter $filter *> $logPath
            if ($LASTEXITCODE -ne 0) {
                throw "Benchmark slice failed for provider '$provider' filter '$filter'. See $logPath."
            }

            $artifactResults = Join-Path $benchmarksDir 'BenchmarkDotNet.Artifacts/results'
            $reports = Get-ChildItem -LiteralPath $artifactResults -File -Filter '*-report.*' -ErrorAction SilentlyContinue
            if (-not $reports) {
                throw "Benchmark slice produced no report files for provider '$provider' filter '$filter'. See $logPath."
            }

            foreach ($report in $reports) {
                $sliceName = '{0}-{1}-{2}' -f $provider, $safeFilter, $report.Name
                Copy-Item -LiteralPath $report.FullName -Destination (Join-Path $sliceResults $report.Name) -Force
                Copy-Item -LiteralPath $report.FullName -Destination (Join-Path $mergedResults $sliceName) -Force
            }
        }
        finally {
            Pop-Location
        }
    }
}

& (Join-Path $root 'eng/benchmark-evidence.ps1') `
    -ResultsDirectory $mergedResults `
    -OutputDirectory (Join-Path $runRoot 'v1-evidence') `
    -BenchmarkFilter ($filterList -join '; ') `
    -Mode 'slice'

if ($CheckThresholds) {
    & (Join-Path $root 'eng/check-benchmark-thresholds.ps1') `
        -ResultsDirectory $mergedResults `
        -OutputDirectory (Join-Path $runRoot 'v1-evidence') `
        -ThresholdFile (Join-Path $root 'eng/benchmark-thresholds.json') `
        -AllowMissingRules
}

Write-Host "Provider benchmark slices written to $runRoot"
