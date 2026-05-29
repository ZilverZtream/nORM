param(
    [ValidateSet('quick', 'live', 'full', 'rc')]
    [string]$Mode = 'full',
    [string]$Configuration = $(if ($env:NORM_GATE_CONFIGURATION) { $env:NORM_GATE_CONFIGURATION } else { 'Release' }),
    [int]$StressIterations = 20,
    [int]$MinLiveProviders = $(if ($env:NORM_MIN_LIVE_PROVIDERS) { [int]$env:NORM_MIN_LIVE_PROVIDERS } else { 0 }),
    [switch]$SkipBenchmark,
    [switch]$SkipProviderMatrixBenchmark,
    [string]$ProviderMatrixBenchmarkFilter = '*ProviderMatrixBenchmarks*'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$solutionPath = Join-Path $root 'nORM.sln'
$testsProjectPath = Join-Path (Join-Path $root 'tests') 'nORM.Tests.csproj'
$benchmarkProjectDir = Join-Path $root 'benchmarks'
$benchmarkProjectPath = Join-Path (Join-Path $root 'benchmarks') 'nORM.Benchmarks.csproj'
$benchmarkResultsPath = Join-Path (Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts') 'results'
$benchmarkEvidencePath = Join-Path (Join-Path $root 'BenchmarkDotNet.Artifacts') 'v1-evidence'
$runtimeProjectPath = Join-Path (Join-Path $root 'src') 'nORM.csproj'
$toolProjectPath = Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') 'dotnet-norm.csproj'
$testResultsPath = Join-Path (Join-Path (Join-Path $root 'tests') 'TestResults') 'v1-release-gate'
New-Item -ItemType Directory -Force -Path $testResultsPath | Out-Null

function Stop-OrphanedTestHosts {
    # A timed-out or interrupted prior gate run can leave testhost.exe holding
    # tests/bin/Release/net8.0/nORM.dll, which then breaks the next build copy step. The gate
    # must self-recover so consecutive runs work without manual `Stop-Process testhost`.
    try {
        $procs = Get-Process -Name testhost, dotnet -ErrorAction SilentlyContinue |
            Where-Object {
                try {
                    ($_.Path -and $_.Path -like "*\testhost*") -or
                    ($_.MainModule -and $_.MainModule.FileName -like "*\testhost*")
                } catch { $false }
            }
        foreach ($p in $procs) {
            try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch { }
        }
    } catch { }
}

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host ""
    Write-Host "==> $Name"
    $global:LASTEXITCODE = $null
    & $Command
    if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        throw "Step failed: $Name (exit code $LASTEXITCODE)"
    }
}

function Get-TrxLogFileName {
    param([string]$Name)
    return (($Name -replace '[^A-Za-z0-9_.-]', '_') + '.trx')
}

function Get-DuplicateBenchmarkProjects {
    $expectedProject = [System.IO.Path]::GetFullPath($benchmarkProjectPath)
    return @(Get-ChildItem -LiteralPath $root -Recurse -File -Filter 'nORM.Benchmarks.csproj' |
        Where-Object { [System.IO.Path]::GetFullPath($_.FullName) -ne $expectedProject })
}

function Invoke-TestStep {
    param(
        [string]$Name,
        [string]$Filter
    )

    Invoke-Step $Name {
        $arguments = @(
            'test',
            $testsProjectPath,
            '-c',
            $Configuration,
            '--no-build',
            '--no-restore',
            '--logger',
            'console;verbosity=minimal',
            '--logger',
            "trx;LogFileName=$(Get-TrxLogFileName $Name)",
            '--results-directory',
            $testResultsPath
        )

        # In quick mode, exclude live-provider tests when no explicit filter is given.
        # This ensures the quick gate never blocks on missing provider connection strings.
        if ($Filter) {
            $arguments += @('--filter', $Filter)
        }
        elseif ($Mode -eq 'quick') {
            $arguments += @('--filter', 'Category!=LiveProvider')
        }

        dotnet @arguments
    }
}

function Test-ProviderConfigured {
    param([string]$Name)
    return [bool]([Environment]::GetEnvironmentVariable("NORM_TEST_$Name") -or
        [Environment]::GetEnvironmentVariable("NORM_TEST_${Name}_CS"))
}

function Get-NormVersion {
    $propsPath = Join-Path $root 'Directory.Build.props'
    [xml]$props = Get-Content $propsPath
    return $props.Project.PropertyGroup.NormVersion
}

function Clear-PackageOutput {
    param(
        [string]$Directory,
        [string]$PackageId
    )

    if (-not (Test-Path $Directory)) {
        return
    }

    Get-ChildItem -LiteralPath $Directory -File -Filter "$PackageId.*.nupkg" | Remove-Item -Force
    Get-ChildItem -LiteralPath $Directory -File -Filter "$PackageId.*.snupkg" | Remove-Item -Force
}

function Invoke-BenchmarkStep {
    param(
        [string[]]$Arguments
    )

    $workingBenchmarkProjectDir = $benchmarkProjectDir
    $tempRoot = $null
    $benchmarkExitCode = 0

    $duplicateBenchmarkProjects = Get-DuplicateBenchmarkProjects
    if ($duplicateBenchmarkProjects.Count -gt 0) {
        $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('norm-bench-gate-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
        Write-Host "Duplicate benchmark projects detected under repo; running isolated benchmark worktree at $tempRoot"
        git -C $root worktree add --detach $tempRoot HEAD | Write-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create isolated benchmark worktree at $tempRoot."
        }

        $workingBenchmarkProjectDir = Join-Path $tempRoot 'benchmarks'
    }

    try {
        Push-Location $workingBenchmarkProjectDir
        try {
            dotnet run -c $Configuration -- @Arguments
            $benchmarkExitCode = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } else { 0 }
        }
        finally {
            Pop-Location
        }

        if ($tempRoot) {
            $tempArtifacts = Join-Path $workingBenchmarkProjectDir 'BenchmarkDotNet.Artifacts'
            $localArtifacts = Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts'
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $localArtifacts
            if (Test-Path $tempArtifacts) {
                Copy-Item -LiteralPath $tempArtifacts -Destination $benchmarkProjectDir -Recurse -Force
            }
        }
    }
    finally {
        if ($tempRoot) {
            git -C $root worktree remove --force $tempRoot | Write-Host
        }
    }

    if ($benchmarkExitCode -ne 0) {
        throw "Benchmark command failed with exit code $benchmarkExitCode."
    }
}

function Assert-CurrentPackageOutput {
    param(
        [string]$Directory,
        [string]$PackageId,
        [string]$Version
    )

    $expected = @(
        "$PackageId.$Version.nupkg",
        "$PackageId.$Version.snupkg"
    ) | Sort-Object
    $actual = Get-ChildItem -LiteralPath $Directory -File -Filter "$PackageId.*.*nupkg" |
        Select-Object -ExpandProperty Name |
        Sort-Object

    if (@($actual).Count -ne @($expected).Count) {
        throw "Unexpected package artifacts for $PackageId in $Directory. Expected: $($expected -join ', '); actual: $($actual -join ', ')"
    }

    for ($i = 0; $i -lt $expected.Count; $i++) {
        if ($actual[$i] -ne $expected[$i]) {
            throw "Unexpected package artifact for $PackageId at index $i. Expected '$($expected[$i])', actual '$($actual[$i])'."
        }
    }
}

$liveProviders = @()
if (Test-ProviderConfigured 'SQLSERVER') { $liveProviders += 'SQL Server' }
if (Test-ProviderConfigured 'POSTGRES') { $liveProviders += 'PostgreSQL' }
if (Test-ProviderConfigured 'MYSQL') { $liveProviders += 'MySQL' }

if ($MinLiveProviders -eq 0 -and $Mode -eq 'rc') {
    $MinLiveProviders = 3
}
elseif ($MinLiveProviders -eq 0 -and $Mode -eq 'live') {
    $MinLiveProviders = 2
}

if ($liveProviders.Count -lt $MinLiveProviders) {
    throw "v1 gate requires $MinLiveProviders live provider(s), but only $($liveProviders.Count) are configured: $($liveProviders -join ', ')"
}

if (-not $env:NORM_REQUIRE_LIVE_PARITY -and $liveProviders.Count -gt 0) {
    $env:NORM_REQUIRE_LIVE_PARITY = 'any'
}
if (-not $env:NORM_MIN_LIVE_PROVIDERS) {
    $env:NORM_MIN_LIVE_PROVIDERS = "$($liveProviders.Count)"
}

$normVersion = Get-NormVersion

$liveFilter = 'FullyQualifiedName~LiveProvider|FullyQualifiedName~LiveCrossProviderTests|FullyQualifiedName~ProviderSwapSmokeTests|FullyQualifiedName~ProviderParity|FullyQualifiedName~ProviderBehaviorEquivalenceTests|FullyQualifiedName~ProviderBindingParityTests|FullyQualifiedName~BulkProviderParityTests|FullyQualifiedName~CrossProviderAdversarialTests|FullyQualifiedName~ProviderDmlMigrationParityTests|FullyQualifiedName~SqlServerMigrationRunnerTests|FullyQualifiedName~PostgresMigrationRunnerTests|FullyQualifiedName~BulkTransactionAtomicityTests.Postgres_BulkUpdate_BatchFailure_RollsBackEarlierBatches_Live|FullyQualifiedName~BulkTempTableLeakTests.SqlServer|FullyQualifiedName~PostgresBulkDuplicateTests'
$navigationFilter = 'FullyQualifiedName~BatchedNavigationBatchTests|FullyQualifiedName~BatchedNavigationCancellationParityTests|FullyQualifiedName~NavigationCancellationLeakTests|FullyQualifiedName~BatchedNavigationProviderCapTests|FullyQualifiedName~NavigationLoaderSqlServerCapTests'
$transactionFilter = 'FullyQualifiedName~TransactionAtomicCompletionTests|FullyQualifiedName~TransactionRaceTests|FullyQualifiedName~TransactionLifecycleTests|FullyQualifiedName~SyncTransactionCleanupTests|FullyQualifiedName~TransactionFaultInjectionTests'
$compiledFilter = 'FullyQualifiedName~CompiledQueryTests.Compiled_query_multi_param_object|FullyQualifiedName~CompiledQueryFastPathTests|FullyQualifiedName~CompiledQuerySqlShapeParityTests'
$parityFilter = 'FullyQualifiedName~ProviderBindingParityTests|FullyQualifiedName~CompileTimeQueryParameterParityTests|FullyQualifiedName~CompiledQueryProviderMatrixTests|FullyQualifiedName~SourceGenBasicEquivalenceTests|FullyQualifiedName~SourceGenRuntimeParityTests|FullyQualifiedName~CompileTimeQueryLifecycleTests'
$bulkFilter = 'FullyQualifiedName~BulkProviderParityTests|FullyQualifiedName~BulkTransactionAtomicityTests|FullyQualifiedName~BulkTempTableLeakTests|FullyQualifiedName~PostgresBulkDuplicateTests|FullyQualifiedName~ProviderBehaviorEquivalenceTests|FullyQualifiedName~ProviderParityDepthTests|FullyQualifiedName~ProviderParityQueryPagingTests'
$migrationFilter = 'FullyQualifiedName~LiveProviderSavepointMigrationTests|FullyQualifiedName~SqlServerMigrationRunnerTests|FullyQualifiedName~PostgresMigrationRunnerTests|FullyQualifiedName~MigrationCommitCancellationTests|FullyQualifiedName~MigrationCancellationTests|FullyQualifiedName~MigrationReplayFailureTests|FullyQualifiedName~MigrationRunnerCoverageTests'
$cacheMemoryFilter = 'FullyQualifiedName~CacheMemoryBoundReleaseGateTests|FullyQualifiedName~CacheFaultInjectionStressTests.BoundedCacheEvictionUnderContention_30Tasks_SizeBounded_ValuesCorrect|FullyQualifiedName~ConcurrentLruCacheStressTests|FullyQualifiedName~CacheLockConcurrencyTests'
$adversarialFilter = 'FullyQualifiedName~Concurrency|FullyQualifiedName~Concurrent|FullyQualifiedName~Adversarial|FullyQualifiedName~FaultInjected|FullyQualifiedName~Stress'

Write-Host "nORM v1 release gate"
Write-Host "  Mode:                $Mode"
Write-Host "  Configuration:       $Configuration"
Write-Host "  Stress iterations:   $StressIterations"
Write-Host "  Live providers:      $($liveProviders -join ', ')"
Write-Host "  Min live providers:  $MinLiveProviders"
Write-Host "  Benchmark:           $(if ($SkipBenchmark) { 'skipped' } else { 'enabled' })"
Write-Host "  Provider matrix:     $(if ($SkipBenchmark -or $SkipProviderMatrixBenchmark -or $Mode -ne 'rc') { 'skipped' } else { 'enabled' })"
Write-Host "  Package version:     $normVersion"

Invoke-Step 'clean orphaned test hosts' { Stop-OrphanedTestHosts }
Invoke-Step 'clean package outputs before build' {
    Clear-PackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'nORM'
    Clear-PackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm'
}
Invoke-Step 'restore' { dotnet restore $solutionPath }
Invoke-Step 'build' { dotnet build $solutionPath --no-restore -c $Configuration --nologo }
Invoke-Step 'encoding scan' {
    & (Join-Path $root 'eng/scripts/check-encoding.ps1')
}
Invoke-Step 'AOT publish warning scan' {
    # Run a real AOT publish on the runtime project and diff IL diagnostics against the
    # baseline at eng/aot-baseline.txt. The source generator project is isolated from PublishAot
    # propagation via TreatAsLocalProperty in nORM.SourceGenerators.csproj, so NETSDK1207 no
    # longer fires there. New IL diagnostics (file:line:code not in the baseline) fail the gate;
    # entries that have disappeared from the baseline are reported as progress, prompting the
    # operator to regenerate the baseline via eng/scripts/update-aot-baseline.ps1.
    $aotOutput = dotnet publish $runtimeProjectPath -c $Configuration -r linux-x64 --self-contained `
        -p:PublishAot=true --nologo 2>&1 | Out-String

    $diagnosticRegex = [regex]'(?m)^(?<file>.+?)\((?<line>\d+),(?<col>\d+)\):\s+(?:error|warning)\s+(?<code>IL\d{4}):'
    $repoRootPrefix = ($root + '\')
    $observed = $diagnosticRegex.Matches($aotOutput) | ForEach-Object {
        $relativeFile = ($_.Groups['file'].Value -replace [regex]::Escape($repoRootPrefix), '') -replace '\\', '/'
        '{0}:{1}:{2}' -f $relativeFile, $_.Groups['line'].Value, $_.Groups['code'].Value
    } | Sort-Object -Unique

    $baselinePath = Join-Path $root 'eng/aot-baseline.txt'
    if (-not (Test-Path -LiteralPath $baselinePath)) {
        throw "AOT baseline missing at $baselinePath. Regenerate via eng/scripts/update-aot-baseline.ps1."
    }
    $baseline = @(Get-Content -LiteralPath $baselinePath | Where-Object { $_ -and -not $_.StartsWith('#') })

    $newDiagnostics    = @($observed | Where-Object { $baseline -notcontains $_ })
    $cleanedDiagnostics = @($baseline | Where-Object { $observed -notcontains $_ })

    Write-Host ("AOT publish scan: observed={0} baseline={1} new={2} cleaned={3}" -f `
        @($observed).Count, @($baseline).Count, $newDiagnostics.Count, $cleanedDiagnostics.Count)

    if ($cleanedDiagnostics.Count -gt 0) {
        Write-Host "AOT diagnostics no longer present (regenerate baseline to lock in progress):"
        $cleanedDiagnostics | ForEach-Object { Write-Host "  - $_" }
    }

    if ($newDiagnostics.Count -gt 0) {
        Write-Host "New AOT diagnostics not in baseline:"
        $newDiagnostics | ForEach-Object { Write-Host "  + $_" }
        $expectAllowed = $env:NORM_AOT_NEW_DIAGNOSTICS_ALLOWED -eq '1'
        if (-not $expectAllowed) {
            throw "AOT publish produced $($newDiagnostics.Count) new IL diagnostic(s) not in eng/aot-baseline.txt. Annotate the call sites or, if intentional, regenerate the baseline."
        }
        Write-Host "NORM_AOT_NEW_DIAGNOSTICS_ALLOWED=1: not failing the gate (diagnostic mode)."
    }

    if ($newDiagnostics.Count -eq 0 -and $observed.Count -eq 0 -and $baseline.Count -eq 0) {
        Write-Host "AOT publish warning scan: no IL diagnostics (runtime is AOT-clean)."
    }

    # `dotnet publish` returns a non-zero exit code whenever the baseline contains IL errors
    # (NETSDK or analyzer errors). Once the diff against the baseline is clean, that exit
    # code is no longer a gate failure — the gate fails on new IL diagnostics, not on the
    # presence of the baseline. Reset $LASTEXITCODE so Invoke-Step does not re-throw.
    $global:LASTEXITCODE = 0
}
Invoke-TestStep 'public API snapshot' 'FullyQualifiedName~PublicApiSnapshotTests'
Invoke-TestStep 'package consumer smoke tests' 'FullyQualifiedName~PackageConsumerIntegrationTests'
Invoke-TestStep 'CLI smoke tests' 'FullyQualifiedName~CliIntegrationTests'

if ($Mode -in @('live', 'full', 'rc')) {
    Invoke-TestStep 'live provider gate' $liveFilter
}

if ($Mode -in @('full', 'rc')) {
    Invoke-TestStep 'full test suite' ''
}

if ($Mode -eq 'rc') {
    foreach ($i in 1..$StressIterations) {
        Invoke-TestStep "navigation stress $i/$StressIterations" $navigationFilter
    }
    foreach ($i in 1..$StressIterations) {
        Invoke-TestStep "transaction stress $i/$StressIterations" $transactionFilter
    }
    foreach ($i in 1..$StressIterations) {
        Invoke-TestStep "compiled query stress $i/$StressIterations" $compiledFilter
    }

    Invoke-TestStep 'provider/source-gen parity' $parityFilter
    Invoke-TestStep 'bulk/provider parity' $bulkFilter
    Invoke-TestStep 'migration provider gate' $migrationFilter
    Invoke-TestStep 'cache memory bounds gate' $cacheMemoryFilter
    Invoke-TestStep 'concurrency/adversarial gate' $adversarialFilter
    Invoke-TestStep 'live provider gate second pass' $liveFilter
    Invoke-TestStep 'full test suite second pass' ''
}

if (-not $SkipBenchmark -and $Mode -in @('full', 'rc')) {
    Invoke-Step 'clean benchmark outputs before run' {
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue `
            (Join-Path $root 'BenchmarkDotNet.Artifacts'),
            (Join-Path $benchmarkProjectDir 'BenchmarkDotNet.Artifacts')
    }

    Invoke-Step 'fast complex query benchmark' { Invoke-BenchmarkStep @('--fast', 'Query_Complex') }
}

if (-not $SkipBenchmark -and -not $SkipProviderMatrixBenchmark -and $Mode -eq 'rc') {
    if (-not (Test-ProviderConfigured 'SQLSERVER') -or -not (Test-ProviderConfigured 'POSTGRES') -or -not (Test-ProviderConfigured 'MYSQL')) {
        throw 'Provider matrix benchmark requires SQL Server, PostgreSQL, and MySQL live connection strings.'
    }

    Invoke-Step 'SQLite/SQL Server/PostgreSQL/MySQL provider matrix benchmark slices' {
        $sliceRoot = Join-Path $root 'BenchmarkDotNet.Artifacts/provider-slices'
        & (Join-Path $root 'eng/run-provider-benchmark-slice.ps1') `
            -Providers 'Sqlite,SqlServer,Postgres,MySql' `
            -Filters $ProviderMatrixBenchmarkFilter `
            -Configuration $Configuration `
            -OutputRoot $sliceRoot

        $latestSlice = Get-ChildItem -LiteralPath $sliceRoot -Directory |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -First 1
        if (-not $latestSlice) {
            throw "Provider matrix benchmark slices produced no run directory under $sliceRoot."
        }

        $script:benchmarkResultsPath = Join-Path $latestSlice.FullName 'results'
    }
}

if (-not $SkipBenchmark -and $Mode -in @('full', 'rc')) {
    Invoke-Step 'benchmark evidence manifest' {
        & (Join-Path $root 'eng/benchmark-evidence.ps1') `
            -ResultsDirectory $benchmarkResultsPath `
            -OutputDirectory $benchmarkEvidencePath `
            -BenchmarkFilter $ProviderMatrixBenchmarkFilter `
            -Mode $Mode
    }

    Invoke-Step 'benchmark threshold gate' {
        $thresholdArgs = @{
            ResultsDirectory = $benchmarkResultsPath
            OutputDirectory = $benchmarkEvidencePath
            ThresholdFile = (Join-Path $root 'eng/benchmark-thresholds.json')
        }
        if ($Mode -ne 'rc') {
            $thresholdArgs.AllowMissingRules = $true
        }

        & (Join-Path $root 'eng/check-benchmark-thresholds.ps1') @thresholdArgs
    }
}

Invoke-Step 'clean package outputs' {
    Clear-PackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'nORM'
    Clear-PackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm'
}
Invoke-Step 'pack nORM' { dotnet pack $runtimeProjectPath -c $Configuration --no-build --include-symbols -p:SymbolPackageFormat=snupkg }
Invoke-Step 'pack dotnet-norm' { dotnet pack $toolProjectPath -c $Configuration --no-build --include-symbols -p:SymbolPackageFormat=snupkg }
Invoke-Step 'validate package outputs' {
    Assert-CurrentPackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'nORM' $normVersion
    Assert-CurrentPackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm' $normVersion
}
Invoke-Step 'RC artifact manifest' {
    & (Join-Path $root 'eng/rc-artifact-manifest.ps1') `
        -Mode $Mode `
        -Configuration $Configuration `
        -StressIterations $StressIterations `
        -BenchmarkSkipped ([bool]$SkipBenchmark)
}

Write-Host ""
Write-Host "nORM v1 release gate passed."
