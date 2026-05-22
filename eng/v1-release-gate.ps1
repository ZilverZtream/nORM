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
$benchmarkProjectPath = Join-Path (Join-Path $root 'benchmarks') 'nORM.Benchmarks.csproj'
$runtimeProjectPath = Join-Path (Join-Path $root 'src') 'nORM.csproj'
$toolProjectPath = Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') 'dotnet-norm.csproj'
$testResultsPath = Join-Path (Join-Path (Join-Path $root 'tests') 'TestResults') 'v1-release-gate'
New-Item -ItemType Directory -Force -Path $testResultsPath | Out-Null

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

        if ($Filter) {
            $arguments += @('--filter', $Filter)
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

Invoke-Step 'clean package outputs before build' {
    Clear-PackageOutput (Join-Path (Join-Path $root 'src') "bin\$Configuration") 'nORM'
    Clear-PackageOutput (Join-Path (Join-Path (Join-Path $root 'src') 'dotnet-norm') "bin\$Configuration") 'dotnet-norm'
}
Invoke-Step 'restore' { dotnet restore $solutionPath }
Invoke-Step 'build' { dotnet build $solutionPath --no-restore -c $Configuration --nologo }
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
    Invoke-Step 'fast complex query benchmark' { dotnet run --project $benchmarkProjectPath -c $Configuration -- --fast Query_Complex }
}

if (-not $SkipBenchmark -and -not $SkipProviderMatrixBenchmark -and $Mode -eq 'rc') {
    if (-not (Test-ProviderConfigured 'SQLSERVER') -or -not (Test-ProviderConfigured 'POSTGRES') -or -not (Test-ProviderConfigured 'MYSQL')) {
        throw 'Provider matrix benchmark requires SQL Server, PostgreSQL, and MySQL live connection strings.'
    }

    Invoke-Step 'SQLite/SQL Server/PostgreSQL/MySQL provider matrix benchmark' {
        dotnet run --project $benchmarkProjectPath -c $Configuration -- --provider-matrix --filter $ProviderMatrixBenchmarkFilter
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

Write-Host ""
Write-Host "nORM v1 release gate passed."
