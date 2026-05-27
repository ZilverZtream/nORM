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
        Assert.Contains("Query_Simple_RawAdo_PreparedOptimized", code);
        Assert.Contains("Query_Complex_EfCore_Compiled", code);
        Assert.Contains("Query_Complex_nORM_Compiled", code);
        Assert.Contains("Query_Complex_RawAdo_Convenience", code);
        Assert.Contains("Query_Complex_RawAdo_Optimized", code);
        Assert.Contains("Query_Complex_RawAdo_PreparedOptimized", code);
        Assert.DoesNotContain("Query_Simple_Dapper_Prepared", code);
        Assert.DoesNotContain("Query_Complex_Dapper_Prepared", code);
    }

    [Fact]
    public void ProviderMatrix_OptimizedRawAdoUsesOrdinalTypedGetterPath()
    {
        var code = ReadRepoFile("benchmarks/ProviderMatrixBenchmarks.cs");

        Assert.Contains("ReadUsersConvenienceAsync", code);
        Assert.Contains("ReadUsersOptimizedAsync", code);
        Assert.Contains("ReadUserConvenience", code);
        Assert.Contains("ReadUserOptimized", code);
        Assert.Contains("reader[\"Id\"]", code);
        Assert.Contains("reader.GetInt32(0)", code);
        Assert.Contains("reader.GetString(1)", code);
        Assert.Contains("reader.GetDouble(8)", code);
        Assert.Contains("[SimpleJob(RuntimeMoniker.Net80, warmupCount: 3, iterationCount: 10)]", code);
        AssertMethodContains(code, "Query_Simple_RawAdo_Optimized", "ReadUsersOptimizedAsync");
        AssertMethodContains(code, "Query_Complex_RawAdo_Optimized", "ReadUsersOptimizedAsync");
        AssertMethodContains(code, "Query_Simple_RawAdo_PreparedOptimized", "ReadUsersOptimizedAsync(_adoSimplePrepared!)");
        AssertMethodContains(code, "Query_Complex_RawAdo_PreparedOptimized", "ReadUsersOptimizedAsync(_adoComplexPrepared!)");
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
        Assert.DoesNotContain("Query_Simple_Dapper_Prepared", code);
        Assert.DoesNotContain("Query_Complex_Dapper_Prepared", code);
    }

    [Theory]
    [InlineData("benchmarks/ProviderMatrixBenchmarks.cs")]
    [InlineData("benchmarks/OrmBenchmarks.cs")]
    public void RuntimeQueryBenchmarks_AreNoTrackingAcrossEfAndNorm(string relativePath)
    {
        var code = ReadRepoFile(relativePath);

        AssertMethodContains(code, "Query_Simple_EfCore", ".AsNoTracking()");
        AssertMethodContains(code, "Query_Simple_nORM", ".AsNoTracking()");
        AssertMethodContains(code, "Query_Complex_EfCore", ".AsNoTracking()");
        AssertMethodContains(code, "Query_Complex_nORM", ".AsNoTracking()");
        AssertMethodContains(code, "Query_Join_EfCore", ".AsNoTracking()");
        AssertMethodContains(code, "Query_Join_nORM", ".AsNoTracking()");
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
        var thresholds = ReadRepoFile("eng/benchmark-thresholds.json");
        var governance = ReadRepoFile("docs/benchmark-governance.md");

        Assert.Contains("benchmark evidence manifest", gate);
        Assert.Contains("eng/benchmark-evidence.ps1", gate);
        Assert.Contains("benchmark threshold gate", gate);
        Assert.Contains("eng/check-benchmark-thresholds.ps1", gate);
        Assert.Contains("BenchmarkDotNet.Artifacts/v1-evidence", governance);
        Assert.Contains("eng/benchmark-thresholds.json", governance);
        Assert.Contains("eng/check-benchmark-thresholds.ps1", governance);
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
        Assert.Contains("Query_Join_nORM_Compiled", thresholds);
        Assert.Contains("BulkInsert_Idiomatic_nORM", thresholds);
        Assert.Contains("Convert-MeanToNanoseconds", thresholdGate);
        Assert.Contains("Convert-AllocatedToBytes", thresholdGate);
        Assert.Contains("Benchmark threshold check failed", thresholdGate);
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
