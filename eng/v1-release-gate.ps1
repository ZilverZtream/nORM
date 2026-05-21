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

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host ""
    Write-Host "==> $Name"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Name (exit code $LASTEXITCODE)"
    }
}

function Test-ProviderConfigured {
    param([string]$Name)
    return [bool]([Environment]::GetEnvironmentVariable("NORM_TEST_$Name") -or
        [Environment]::GetEnvironmentVariable("NORM_TEST_${Name}_CS"))
}

$liveProviders = @()
if (Test-ProviderConfigured 'SQLSERVER') { $liveProviders += 'SQL Server' }
if (Test-ProviderConfigured 'POSTGRES') { $liveProviders += 'PostgreSQL' }
if (Test-ProviderConfigured 'MYSQL') { $liveProviders += 'MySQL' }

if ($MinLiveProviders -eq 0 -and ($Mode -eq 'rc' -or $Mode -eq 'live')) {
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

$liveFilter = 'FullyQualifiedName~LiveProvider|FullyQualifiedName~LiveCrossProviderTests|FullyQualifiedName~ProviderSwapSmokeTests|FullyQualifiedName~ProviderParity|FullyQualifiedName~ProviderBehaviorEquivalenceTests|FullyQualifiedName~ProviderBindingParityTests|FullyQualifiedName~BulkProviderParityTests|FullyQualifiedName~CrossProviderAdversarialTests|FullyQualifiedName~ProviderDmlMigrationParityTests|FullyQualifiedName~SqlServerMigrationRunnerTests|FullyQualifiedName~PostgresMigrationRunnerTests|FullyQualifiedName~BulkTransactionAtomicityTests.Postgres_BulkUpdate_BatchFailure_RollsBackEarlierBatches_Live|FullyQualifiedName~BulkTempTableLeakTests.SqlServer|FullyQualifiedName~PostgresBulkDuplicateTests'
$navigationFilter = 'FullyQualifiedName~BatchedNavigationBatchTests|FullyQualifiedName~BatchedNavigationCancellationParityTests|FullyQualifiedName~NavigationCancellationLeakTests|FullyQualifiedName~BatchedNavigationProviderCapTests|FullyQualifiedName~NavigationLoaderSqlServerCapTests'
$transactionFilter = 'FullyQualifiedName~TransactionAtomicCompletionTests|FullyQualifiedName~TransactionRaceTests|FullyQualifiedName~TransactionLifecycleTests|FullyQualifiedName~SyncTransactionCleanupTests|FullyQualifiedName~TransactionFaultInjectionTests'
$compiledFilter = 'FullyQualifiedName~CompiledQueryTests.Compiled_query_multi_param_object|FullyQualifiedName~CompiledQueryFastPathTests|FullyQualifiedName~CompiledQuerySqlShapeParityTests'
$parityFilter = 'FullyQualifiedName~ProviderBindingParityTests|FullyQualifiedName~CompileTimeQueryParameterParityTests|FullyQualifiedName~CompiledQueryProviderMatrixTests|FullyQualifiedName~SourceGenBasicEquivalenceTests|FullyQualifiedName~SourceGenRuntimeParityTests|FullyQualifiedName~CompileTimeQueryLifecycleTests'
$bulkFilter = 'FullyQualifiedName~BulkProviderParityTests|FullyQualifiedName~BulkTransactionAtomicityTests|FullyQualifiedName~BulkTempTableLeakTests|FullyQualifiedName~PostgresBulkDuplicateTests|FullyQualifiedName~ProviderBehaviorEquivalenceTests|FullyQualifiedName~ProviderParityDepthTests|FullyQualifiedName~ProviderParityQueryPagingTests'
$migrationFilter = 'FullyQualifiedName~LiveProviderSavepointMigrationTests|FullyQualifiedName~SqlServerMigrationRunnerTests|FullyQualifiedName~PostgresMigrationRunnerTests|FullyQualifiedName~MigrationCommitCancellationTests|FullyQualifiedName~MigrationCancellationTests|FullyQualifiedName~MigrationReplayFailureTests|FullyQualifiedName~MigrationRunnerCoverageTests'
$adversarialFilter = 'FullyQualifiedName~Concurrency|FullyQualifiedName~Concurrent|FullyQualifiedName~Adversarial|FullyQualifiedName~FaultInjected|FullyQualifiedName~Stress'

Write-Host "nORM v1 release gate"
Write-Host "  Mode:                $Mode"
Write-Host "  Configuration:       $Configuration"
Write-Host "  Stress iterations:   $StressIterations"
Write-Host "  Live providers:      $($liveProviders -join ', ')"
Write-Host "  Min live providers:  $MinLiveProviders"
Write-Host "  Benchmark:           $(if ($SkipBenchmark) { 'skipped' } else { 'enabled' })"
Write-Host "  Provider matrix:     $(if ($SkipBenchmark -or $SkipProviderMatrixBenchmark -or $Mode -ne 'rc') { 'skipped' } else { 'enabled' })"

Invoke-Step 'restore' { dotnet restore nORM.sln }
Invoke-Step 'build' { dotnet build nORM.sln --no-restore -c $Configuration --nologo }
Invoke-Step 'public API snapshot' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter 'FullyQualifiedName~PublicApiSnapshotTests' --logger 'console;verbosity=minimal' }

if ($Mode -in @('live', 'full', 'rc')) {
    Invoke-Step 'live provider gate' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $liveFilter --logger 'console;verbosity=minimal' }
}

if ($Mode -in @('full', 'rc')) {
    Invoke-Step 'full test suite' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --logger 'console;verbosity=minimal' }
}

if ($Mode -eq 'rc') {
    foreach ($i in 1..$StressIterations) {
        Invoke-Step "navigation stress $i/$StressIterations" { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $navigationFilter --logger 'console;verbosity=minimal' }
    }
    foreach ($i in 1..$StressIterations) {
        Invoke-Step "transaction stress $i/$StressIterations" { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $transactionFilter --logger 'console;verbosity=minimal' }
    }
    foreach ($i in 1..$StressIterations) {
        Invoke-Step "compiled query stress $i/$StressIterations" { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $compiledFilter --logger 'console;verbosity=minimal' }
    }

    Invoke-Step 'provider/source-gen parity' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $parityFilter --logger 'console;verbosity=minimal' }
    Invoke-Step 'bulk/provider parity' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $bulkFilter --logger 'console;verbosity=minimal' }
    Invoke-Step 'migration provider gate' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $migrationFilter --logger 'console;verbosity=minimal' }
    Invoke-Step 'concurrency/adversarial gate' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $adversarialFilter --logger 'console;verbosity=minimal' }
    Invoke-Step 'live provider gate second pass' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --filter $liveFilter --logger 'console;verbosity=minimal' }
    Invoke-Step 'full test suite second pass' { dotnet test tests\nORM.Tests.csproj -c $Configuration --no-build --no-restore --logger 'console;verbosity=minimal' }
}

if (-not $SkipBenchmark -and $Mode -in @('full', 'rc')) {
    Invoke-Step 'fast complex query benchmark' { dotnet run --project benchmarks\nORM.Benchmarks.csproj -c $Configuration -- --fast Query_Complex }
}

if (-not $SkipBenchmark -and -not $SkipProviderMatrixBenchmark -and $Mode -eq 'rc') {
    if (-not (Test-ProviderConfigured 'SQLSERVER') -or -not (Test-ProviderConfigured 'POSTGRES')) {
        throw 'Provider matrix benchmark requires both SQL Server and PostgreSQL live connection strings.'
    }

    Invoke-Step 'SQLite/SQL Server/PostgreSQL provider matrix benchmark' {
        dotnet run --project benchmarks\nORM.Benchmarks.csproj -c $Configuration -- --provider-matrix --filter $ProviderMatrixBenchmarkFilter
    }
}

Invoke-Step 'pack nORM' { dotnet pack src\nORM.csproj -c $Configuration --no-build --include-symbols -p:SymbolPackageFormat=snupkg }
Invoke-Step 'pack dotnet-norm' { dotnet pack src\dotnet-norm\dotnet-norm.csproj -c $Configuration --no-build --include-symbols -p:SymbolPackageFormat=snupkg }

Write-Host ""
Write-Host "nORM v1 release gate passed."
