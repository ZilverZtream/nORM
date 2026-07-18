using System;
using System.IO;
using System.Text.RegularExpressions;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public sealed class BenchmarkFairnessLockTests
{
    private static readonly string RepoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));

    [Theory]
    [InlineData("benchmarks/ProviderMatrixBenchmarks.cs", "s_normJoinCompiled")]
    [InlineData("benchmarks/OrmBenchmarks.cs", "_normJoinCompiled")]
    public void JoinBenchmarks_UseSameTypedDtoShape(string relativePath, string compiledFieldName)
    {
        var code = ReadRepoFile(relativePath);

        Assert.Contains("public Task<List<BenchmarkJoinRow>> Query_Join_EfCore()", code);
        Assert.Contains("public Task<List<BenchmarkJoinRow>> Query_Join_nORM()", code);
        Assert.Contains("public async Task<List<BenchmarkJoinRow>> Query_Join_Dapper()", code);
        Assert.Contains("public Task<List<BenchmarkJoinRow>> Query_Join_RawAdo_Optimized()", code);
        Assert.Contains("public Task<List<BenchmarkJoinRow>> Query_Join_nORM_Compiled()", code);

        Assert.Contains($"Func<nORM.Core.DbContext, int, Task<List<BenchmarkJoinRow>>> {compiledFieldName}", code);
        Assert.Contains("new { u.Name, o.Amount, o.ProductName }", code);
        Assert.Contains("new BenchmarkJoinRow(x.Name, x.Amount, x.ProductName)", code);
        Assert.Contains("QueryAsync<BenchmarkJoinRow>", code);

        Assert.DoesNotContain("QueryAsync(QueryJoinSql()", code);
        Assert.DoesNotContain("QueryAsync(sql, new { Amount = 100 })", code);
    }

    [Fact]
    public void FastNormJoinBenchmarks_UseSameTypedDtoShape()
    {
        var code = ReadRepoFile("benchmarks/FastNormBenchmarks.cs");

        Assert.Contains("Func<nORM.Core.DbContext, int, Task<List<BenchmarkJoinRow>>>? _normJoinCompiled", code);
        Assert.Contains("public Task<List<BenchmarkJoinRow>> Query_Join()", code);
        Assert.Contains("public Task<List<BenchmarkJoinRow>> Query_Join_Compiled()", code);
        Assert.Contains("new BenchmarkJoinRow(u.Name, o.Amount, o.ProductName)", code);
        Assert.DoesNotContain("Task<List<object>>? _normJoinCompiled", code);
        Assert.DoesNotContain("public async Task<List<object>> Query_Join()", code);
        Assert.DoesNotContain("public Task<List<object>> Query_Join_Compiled()", code);
    }

    [Fact]
    public void ProviderMatrix_SeparatesComparableExecutionModes()
    {
        var code = ReadRepoFile("benchmarks/ProviderMatrixBenchmarks.cs");

        Assert.Contains("[ParamsSource(nameof(Providers))]", code);
        Assert.Contains("public static IReadOnlyList<string> SelectedProviders", code);
        Assert.Contains("NORM_TEST_MYSQL", code);
        Assert.Contains("Query_Simple_EfCore_Compiled", code);
        Assert.Contains("Query_Simple_nORM_Compiled", code);
        Assert.Contains("Query_Simple_RawAdo_Convenience", code);
        Assert.Contains("Query_Simple_RawAdo_Optimized", code);
        Assert.Contains("Query_Simple_RawAdo_TypedNoBox", code);
        Assert.Contains("Query_Simple_RawAdo_PreparedOptimized", code);
        Assert.Contains("Query_Simple_RawAdo_PreparedTypedNoBox", code);
        Assert.Contains("Query_Complex_EfCore_Compiled", code);
        Assert.Contains("Query_Complex_nORM_Compiled", code);
        Assert.Contains("Query_Complex_RawAdo_Convenience", code);
        Assert.Contains("Query_Complex_RawAdo_Optimized", code);
        Assert.Contains("Query_Complex_RawAdo_TypedNoBox", code);
        Assert.Contains("Query_Complex_RawAdo_PreparedOptimized", code);
        Assert.Contains("Query_Complex_RawAdo_PreparedTypedNoBox", code);
        Assert.Contains("Query_Scale1k_nORM", code);
        Assert.Contains("Query_Scale1k_Dapper", code);
        Assert.Contains("Query_Scale1k_RawAdo_TypedNoBox", code);
        Assert.Contains("Query_Scale10k_nORM", code);
        Assert.Contains("Query_Scale10k_Dapper", code);
        Assert.Contains("Query_Scale10k_RawAdo_TypedNoBox", code);
        Assert.Contains("Query_ParallelThroughput_nORM", code);
        Assert.Contains("Query_ParallelThroughput_Dapper", code);
        Assert.Contains("Query_ParallelThroughput_RawAdo_TypedNoBox", code);
        Assert.DoesNotContain("Query_Simple_Dapper_Prepared", code);
        Assert.DoesNotContain("Query_Complex_Dapper_Prepared", code);
    }

    [Fact]
    public void ProviderMatrix_OptimizedRawAdoUsesOrdinalTypedGetterPath()
    {
        var code = ReadRepoFile("benchmarks/ProviderMatrixBenchmarks.cs");

        Assert.Contains("ReadUsersConvenienceAsync", code);
        Assert.Contains("ReadUsersOptimizedAsync", code);
        Assert.Contains("ReadUsersTypedNoBoxAsync", code);
        Assert.Contains("ReadUserConvenience", code);
        Assert.Contains("ReadUserOptimized", code);
        Assert.Contains("ReadUserTypedNoBox", code);
        Assert.Contains("reader[\"Id\"]", code);
        Assert.Contains("reader.GetInt32(0)", code);
        Assert.Contains("reader.GetString(1)", code);
        Assert.Contains("reader.GetDateTime(3)", code);
        Assert.Contains("reader.GetBoolean(4)", code);
        Assert.Contains("reader.GetDouble(8)", code);
        Assert.Contains("[SimpleJob(RuntimeMoniker.Net80, launchCount: 3, warmupCount: 3, iterationCount: 20)]", code);
        Assert.Contains("ApplyBenchmarkConnectionSettingsAsync(Provider, _efContext.Database.GetDbConnection())", code);
        Assert.Contains("OpenBenchmarkConnectionAsync(Provider, _connectionString)", code);
        Assert.Contains("PRAGMA synchronous = NORMAL", code);
        Assert.Contains("PRAGMA busy_timeout = 5000", code);
        Assert.Contains("private const int UserCount = 12_000", code);
        AssertMethodContains(code, "Query_Simple_RawAdo_Optimized", "ReadUsersOptimizedAsync");
        AssertMethodContains(code, "Query_Simple_RawAdo_TypedNoBox", "ReadUsersTypedNoBoxAsync");
        AssertMethodContains(code, "Query_Complex_RawAdo_Optimized", "ReadUsersOptimizedAsync");
        AssertMethodContains(code, "Query_Complex_RawAdo_TypedNoBox", "ReadUsersTypedNoBoxAsync");
        AssertMethodContains(code, "Query_Simple_RawAdo_PreparedOptimized", "ReadUsersOptimizedAsync(_adoSimplePrepared!)");
        AssertMethodContains(code, "Query_Simple_RawAdo_PreparedTypedNoBox", "ReadUsersTypedNoBoxAsync(_adoSimplePrepared!)");
        AssertMethodContains(code, "Query_Complex_RawAdo_PreparedOptimized", "ReadUsersOptimizedAsync(_adoComplexPrepared!)");
        AssertMethodContains(code, "Query_Complex_RawAdo_PreparedTypedNoBox", "ReadUsersTypedNoBoxAsync(_adoComplexPrepared!)");
    }

    [Fact]
    public void SqliteComparison_UsesExplicitRawAdoCategories()
    {
        var code = ReadRepoFile("benchmarks/OrmBenchmarks.cs");

        Assert.Contains("Query_Simple_RawAdo_Convenience", code);
        Assert.Contains("Query_Simple_RawAdo_Optimized", code);
        Assert.Contains("Query_Simple_RawAdo_PreparedOptimized", code);
        Assert.Contains("Query_Complex_RawAdo_Convenience", code);
        Assert.Contains("Query_Complex_RawAdo_Optimized", code);
        Assert.Contains("Query_Complex_RawAdo_PreparedOptimized", code);
        Assert.Contains("ReadUserOptimized", code);
        Assert.Contains("reader.GetInt32(0)", code);
        Assert.Contains("PRAGMA synchronous = NORMAL", code);
        Assert.Contains("PRAGMA busy_timeout = 5000", code);
        Assert.DoesNotContain("Query_Simple_Dapper_Prepared", code);
        Assert.DoesNotContain("Query_Complex_Dapper_Prepared", code);
    }

    [Theory]
    [InlineData("benchmarks/ProviderMatrixBenchmarks.cs")]
    [InlineData("benchmarks/OrmBenchmarks.cs")]
    public void RuntimeQueryBenchmarks_AreNoTrackingAcrossEfAndNorm(string relativePath)
    {
        var code = ReadRepoFile(relativePath);

        // Fairness: the runtime query benchmarks must read no-tracking on BOTH sides, so neither pays
        // change-tracking overhead. nORM and EF each define an AsNoTracking(IQueryable<T>) extension, which
        // makes a fluent q.AsNoTracking() ambiguous in a file that references both. Each side therefore
        // applies no-tracking unambiguously: EF via EfQueryableExtensions.AsNoTracking(...) on every runtime
        // query, and nORM via the context's NoTracking default (so its query methods don't repeat it per call).
        AssertMethodContains(code, "Query_Simple_EfCore", "EfQueryableExtensions.AsNoTracking(");
        AssertMethodContains(code, "Query_Complex_EfCore", "EfQueryableExtensions.AsNoTracking(");
        AssertMethodContains(code, "Query_Join_EfCore", "EfQueryableExtensions.AsNoTracking(");

        // nORM reads are no-tracking by context default, and nothing re-enables tracking per query.
        Assert.Contains("DefaultTrackingBehavior = nORM.Core.QueryTrackingBehavior.NoTracking", code);
        Assert.DoesNotContain(".AsTracking(", code);
    }

    [Fact]
    public void BenchmarkJoinRow_IsTypedDtoWithDapperAndProjectionConstructors()
    {
        var code = ReadRepoFile("benchmarks/SharedEntities.cs");

        Assert.Contains("public sealed class BenchmarkJoinRow", code);
        Assert.Contains("public BenchmarkJoinRow()", code);
        Assert.Contains("public BenchmarkJoinRow(string name, decimal amount, string productName)", code);
        Assert.Contains("public string Name { get; set; }", code);
        Assert.Contains("public decimal Amount { get; set; }", code);
        Assert.Contains("public string ProductName { get; set; }", code);
    }

    [Fact]
    public void ReleaseGate_GeneratesBenchmarkEvidenceManifest()
    {
        var gate = ReadRepoFile("eng/v1-release-gate.ps1");
        var evidence = ReadRepoFile("eng/benchmark-evidence.ps1");
        var thresholdGate = ReadRepoFile("eng/check-benchmark-thresholds.ps1");
        var isolatedRunner = ReadRepoFile("eng/run-benchmark-isolated.ps1");
        var sliceRunner = ReadRepoFile("eng/run-provider-benchmark-slice.ps1");
        var thresholds = ReadRepoFile("eng/benchmark-thresholds.json");
        var governance = ReadRepoFile("docs/benchmark-governance.md");

        Assert.Contains("benchmark evidence manifest", gate);
        Assert.Contains("eng/benchmark-evidence.ps1", gate);
        Assert.Contains("Assert-CleanReleaseEvidenceWorkspace", gate);
        Assert.Contains("Mode -eq 'rc' -and -not $SkipBenchmark", gate);
        Assert.Contains("RC benchmark evidence", gate);
        Assert.Contains("requires a clean git working tree before collecting release benchmark evidence", gate);
        Assert.Contains("Assert-CleanReleaseEvidenceWorkspace", evidence);
        Assert.Contains("Mode -in @('rc', 'full')", evidence);
        Assert.Contains("requires a clean git working tree before collecting release benchmark evidence", evidence);
        Assert.Contains("$benchmarkEvidenceMode = if ($Mode -eq 'full') { 'smoke' } else { $Mode }", gate);
        Assert.Contains("$benchmarkEvidenceFilter = if ($Mode -eq 'full') { '--fast Query_Complex' } else { $ProviderMatrixBenchmarkFilter }", gate);
        Assert.Contains("-BenchmarkFilter $benchmarkEvidenceFilter", gate);
        Assert.Contains("-Mode $benchmarkEvidenceMode", gate);
        Assert.Contains("benchmark threshold gate", gate);
        Assert.Contains("eng/check-benchmark-thresholds.ps1", gate);
        Assert.Contains("Get-DuplicateBenchmarkProjects", gate);
        Assert.Contains("running isolated benchmark workspace", gate);
        Assert.Contains("Copy-CurrentWorkspaceForBenchmarkIsolation", gate);
        Assert.Contains("ProviderMatrixSliceTimeoutMinutes", gate);
        Assert.Contains("NORM_PROVIDER_MATRIX_SLICE_TIMEOUT_MINUTES", gate);
        Assert.Contains("BenchmarkStepTimeoutMinutes", gate);
        Assert.Contains("NORM_BENCHMARK_STEP_TIMEOUT_MINUTES", gate);
        Assert.Contains("TestStepTimeoutMinutes", gate);
        Assert.Contains("NORM_TEST_STEP_TIMEOUT_MINUTES", gate);
        Assert.Contains("Invoke-TimedBenchmarkProcess", gate);
        Assert.Contains(".dotnet-home", gate);
        Assert.Contains(".dotnet-home", sliceRunner);
        Assert.Contains("'.exitcode'", sliceRunner);
        Assert.Contains("'.args.json'", sliceRunner);
        Assert.Contains(".exitcode", gate);
        Assert.Contains(".args.json", gate);
        Assert.Contains("ConvertFrom-Json", gate);
        Assert.Contains("ConvertFrom-Json", sliceRunner);
        Assert.Contains("Benchmark command finished but did not report an exit code", gate);
        Assert.Contains("Test step '$Name' finished but did not report an exit code", gate);
        Assert.Contains("Benchmark slice finished but did not report an exit code", sliceRunner);
        Assert.Contains("'/NFL'", gate);
        Assert.Contains("'/NDL'", gate);
        Assert.Contains("'/NP'", gate);
        Assert.Contains("Benchmark command exceeded", gate);
        Assert.Contains("Test step '$Name' exceeded", gate);
        Assert.Contains("Benchmark still running after", gate);
        Assert.Contains("-SliceTimeoutMinutes $ProviderMatrixSliceTimeoutMinutes", gate);
        Assert.Contains("Benchmark command failed with exit code", gate);
        Assert.Contains("uncommitted changes", isolatedRunner);
        Assert.Contains("Commit/stash them before collecting release benchmark evidence", isolatedRunner);
        Assert.Contains("BenchmarkDotNet.Artifacts/v1-evidence", governance);
        Assert.Contains("Scheduling And Time Bounds", governance);
        Assert.Contains("daytime correctness validation", governance);
        Assert.Contains("dirty working tree before minting release-grade benchmark evidence", governance);
        Assert.Contains("NORM_BENCHMARK_STEP_TIMEOUT_MINUTES", governance);
        Assert.Contains("NORM_PROVIDER_MATRIX_SLICE_TIMEOUT_MINUTES", governance);
        Assert.Contains("BulkInsert_Idiomatic_*", governance);
        Assert.Contains("Tx + per row", governance);
        Assert.Contains("eng/benchmark-thresholds.json", governance);
        Assert.Contains("eng/check-benchmark-thresholds.ps1", governance);
        Assert.Contains("TenantTemporalBenchmarks", governance);
        Assert.Contains("RawAdo_TypedNoBox", governance);
        Assert.Contains("12,000 users in the provider matrix", ReadRepoFile("benchmarks/README.md"));
        Assert.Contains("Query_Scale10k_*", governance);
        Assert.Contains("Query_ParallelThroughput_*", governance);
        Assert.Contains("synchronous = NORMAL", governance);
        Assert.Contains("launchCount: 3", governance);
        Assert.Contains("FastNormBenchmarks", evidence);
        Assert.Contains("cannot be used as public release evidence", evidence);
        Assert.Contains("evidence mode `smoke`", governance);
        Assert.Contains("'--provider-matrix'", sliceRunner);
        Assert.Contains("'--provider'", sliceRunner);
        Assert.Contains("SliceTimeoutMinutes", sliceRunner);
        Assert.Contains("Benchmark slice timed out", sliceRunner);
        Assert.Contains("Get-LogTail", sliceRunner);
        Assert.Contains("Copy-CurrentWorkspaceForBenchmarkIsolation", sliceRunner);
        Assert.Contains("running isolated benchmark workspace", sliceRunner);
        Assert.Contains("-AllowMissingRules", sliceRunner);
        Assert.Contains("provider-slices", sliceRunner);
        Assert.Contains("Redact-ConnectionString", evidence);
        Assert.Contains("NORM_TEST_SQLSERVER", evidence);
        Assert.Contains("NORM_TEST_POSTGRES", evidence);
        Assert.Contains("NORM_TEST_MYSQL", evidence);
        Assert.Contains("FastestByProvider", evidence);
        Assert.Contains("DriverPackages", evidence);
        Assert.DoesNotContain("Password=$env", evidence);
        Assert.Contains("maxMeanRatio", thresholds);
        Assert.Contains("maxAllocatedRatio", thresholds);
        Assert.Contains("Query_Complex_nORM", thresholds);
        Assert.Contains("Query_Join_nORM", thresholds);
        Assert.Contains("Query_Join_nORM_Compiled", thresholds);
        Assert.Contains("Count_nORM", thresholds);
        Assert.Contains("Insert_Single_nORM", thresholds);
        Assert.Contains("BulkInsert_Idiomatic_nORM", thresholds);
        Assert.Contains("Query_Simple_RawAdo_TypedNoBox", thresholds);
        Assert.Contains("Query_Simple_RawAdo_PreparedTypedNoBox", thresholds);
        Assert.Contains("Query_Complex_RawAdo_TypedNoBox", thresholds);
        Assert.Contains("Query_Complex_RawAdo_PreparedTypedNoBox", thresholds);
        Assert.Contains("Convert-MeanToNanoseconds", thresholdGate);
        Assert.Contains("Convert-AllocatedToBytes", thresholdGate);
        Assert.Contains("Format-InvariantNumber", thresholdGate);
        Assert.Contains("Benchmark threshold check failed", thresholdGate);
        Assert.Single(Regex.Matches(thresholdGate, "function Convert-MeanToNanoseconds"));
        Assert.Contains("if ($Mode -ne 'rc')", gate);
        Assert.Contains("$thresholdArgs.AllowMissingRules = $true", gate);
    }

    [Fact]
    public void BenchmarkEvidence_CarriesSourceCommitAcrossIsolatedProviderSlices()
    {
        var evidence = ReadRepoFile("eng/benchmark-evidence.ps1");
        var sliceRunner = ReadRepoFile("eng/run-provider-benchmark-slice.ps1");

        Assert.Contains("Get-BenchmarkEvidenceCommit", evidence);
        Assert.Contains("Get-BenchmarkEvidenceDisplayPath", evidence);
        Assert.Contains("NORM_BENCHMARK_COMMIT", evidence);
        Assert.Contains("Benchmark evidence requires git commit metadata", evidence);
        Assert.DoesNotContain("$csvFile.FullName.Substring($root.Length)", evidence);
        Assert.Contains("Get-SourceCommitForBenchmarkEvidence", sliceRunner);
        Assert.Contains("NORM_BENCHMARK_COMMIT", sliceRunner);
        Assert.Contains("[Environment]::SetEnvironmentVariable('NORM_BENCHMARK_COMMIT', $benchmarkCommit, 'Process')", sliceRunner);
        Assert.Contains("[Environment]::SetEnvironmentVariable('NORM_BENCHMARK_COMMIT', $previousBenchmarkCommit, 'Process')", sliceRunner);
    }

    [Fact]
    public void TenantTemporalBenchmarks_MeasureLikeForLikeTenantOverheadAndSplitTemporalWrites()
    {
        var code = ReadRepoFile("benchmarks/TenantTemporalBenchmarks.cs");

        Assert.DoesNotContain("[InProcess]", code);
        Assert.Contains("[SimpleJob(RuntimeMoniker.Net80, launchCount: 2, warmupCount: 3, iterationCount: 10)]", code);
        Assert.Contains("Query_ManualTenantPredicate_CountActive", code);
        Assert.Contains("Query_InjectedTenantPredicate_CountActive", code);
        AssertMethodContains(code, "Query_ManualTenantPredicate_CountActive", ".Where(p => p.TenantId == 1)");
        AssertMethodContains(code, "Query_InjectedTenantPredicate_CountActive", "shared.TenantContext.Query<TtbProduct>().CountAsync(p => p.IsActive)");
        Assert.Contains("Insert_NoTemporal", code);
        Assert.Contains("Insert_Temporal", code);
        Assert.Contains("Update_NoTemporal", code);
        Assert.Contains("Update_Temporal", code);
        Assert.Contains("Delete_NoTemporal", code);
        Assert.Contains("Delete_Temporal", code);
        Assert.DoesNotContain("InsertUpdateDelete_NoTemporal", code);
        Assert.DoesNotContain("InsertUpdateDelete_Temporal", code);
        Assert.Contains("PRAGMA synchronous = NORMAL", code);
        Assert.Contains("PRAGMA busy_timeout = 5000", code);
    }

    [Fact]
    public void BenchmarkProgram_DoesNotSwallowBenchmarkFailures()
    {
        var code = ReadRepoFile("benchmarks/Program.cs");

        Assert.Contains("Fast nORM benchmark validation failed", code);
        Assert.Contains("Filtered benchmark validation failed", code);
        Assert.Contains("Provider matrix benchmark validation failed", code);
        Assert.Contains("Console.WriteLine($\"❌ Error running benchmarks: {ex.Message}\");", code);
        Assert.Contains("Console.WriteLine($\"❌ Error running fast benchmarks: {ex.Message}\");", code);
        Assert.True(Regex.Matches(code, @"catch\s*\(Exception ex\)[\s\S]*?throw;").Count >= 2);
    }

    [Fact]
    public void RcArtifactManifest_CleansStalePackageBundleBeforeCopyingCurrentPackages()
    {
        var manifest = ReadRepoFile("eng/rc-artifact-manifest.ps1");

        Assert.Contains("Get-ChildItem -LiteralPath $pkgBundleDir -File -Filter '*.nupkg'", manifest);
        Assert.Contains("Get-ChildItem -LiteralPath $pkgBundleDir -File -Filter '*.snupkg'", manifest);
        Assert.Contains("Remove-Item -Force", manifest);
        Assert.Contains("Copy-Item -LiteralPath $src -Destination $pkgBundleDir -Force", manifest);
    }

    [Fact]
    public void RcArtifactManifest_RequiresBenchmarkEvidenceForBenchmarkEnabledRcManifests()
    {
        var manifest = ReadRepoFile("eng/rc-artifact-manifest.ps1");
        var gates = ReadRepoFile("docs/release-gates.md");

        Assert.Contains("Assert-RcReleaseEvidenceComplete", manifest);
        Assert.Contains("$Mode -ne 'rc' -or $BenchmarkSkipped", manifest);
        Assert.Contains("Benchmark-enabled RC artifact manifests require a clean git working tree", manifest);
        Assert.Contains("BenchmarkDotNet.Artifacts/v1-evidence/benchmark-evidence.json", manifest);
        Assert.Contains("BenchmarkDotNet.Artifacts/v1-evidence/benchmark-evidence.md", manifest);
        Assert.Contains("BenchmarkDotNet.Artifacts/v1-evidence/benchmark-thresholds.json", manifest);
        Assert.Contains("BenchmarkDotNet.Artifacts/v1-evidence/benchmark-thresholds.md", manifest);
        Assert.Contains("*-report.csv", manifest);
        Assert.Contains("benchmark evidence, threshold summaries, and raw BenchmarkDotNet CSV reports", manifest);
        Assert.Contains("refuses `BenchmarkSkipped=false` unless the bundle includes benchmark evidence", gates);
    }

    private static void AssertMethodContains(string code, string methodName, string expected)
    {
        var pattern = $@"public\s+(?:async\s+)?(?:Task<[^>]+>|Task|ValueTask<[^>]+>|[A-Za-z0-9_<>,\s]+)\s+{Regex.Escape(methodName)}\s*\([^)]*\)\s*(?:=>|{{)";
        var match = Regex.Match(code, pattern);
        Assert.True(match.Success, $"Could not find method '{methodName}'.");

        var bodyStart = match.Index;
        var nextBenchmark = code.IndexOf("[Benchmark", bodyStart + match.Length, StringComparison.Ordinal);
        var body = nextBenchmark >= 0 ? code[bodyStart..nextBenchmark] : code[bodyStart..];
        Assert.Contains(expected, body);
    }

    private static string ReadRepoFile(string relativePath)
    {
        var fullPath = Path.Combine(RepoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(fullPath), $"Missing file: {fullPath}");
        return File.ReadAllText(fullPath);
    }
}
