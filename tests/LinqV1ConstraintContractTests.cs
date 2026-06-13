using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the LINQ matrix in <c>docs/linq-support.md</c> as the v1 contract for query-shape
/// support. The matrix must not silently drift - either by claiming shapes that aren't
/// implemented or by dropping the documented Constrained/Supported labels readers depend on.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqV1ConstraintContractTests
{
    private static string Doc()
    {
        var asmDir = Path.GetDirectoryName(typeof(LinqV1ConstraintContractTests).Assembly.Location)!;
        var repoRoot = Path.GetFullPath(Path.Combine(asmDir, "..", "..", "..", ".."));
        return File.ReadAllText(Path.Combine(repoRoot, "docs", "linq-support.md"));
    }

    [Theory]
    [InlineData("`Where` predicates", "Supported")]
    [InlineData("`OrderBy`, `ThenBy`", "Supported")]
    [InlineData("`Skip`, `Take`", "Supported")]
    [InlineData("`Distinct`", "Supported")]
    [InlineData("`Sum`, `Average`, `Min`, `Max`", "Supported")]
    [InlineData("Inner joins", "Supported")]
    [InlineData("Set operations: `Union`, `Intersect`, `Except`, `Concat`", "Supported")]
    [InlineData("`AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf`", "Supported")]
    [InlineData("Group joins", "Supported")]
    [InlineData("`AsAsyncEnumerable`", "Supported")]
    [InlineData("`Include`, `ThenInclude`", "Supported")]
    [InlineData("`SelectMany`", "Supported")]
    [InlineData("`ExecuteUpdateAsync`", "Supported")]
    [InlineData("`ExecuteDeleteAsync`", "Supported")]
    [InlineData("`GroupBy`", "Supported")]
    public void Matrix_marks_v1_supported_shape(string feature, string status)
    {
        var doc = Doc();
        var probe = $"| {feature} | {status} |";
        Assert.Contains(probe, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Matrix_documents_unsupported_client_evaluation_shape()
    {
        var doc = Doc();
        Assert.Contains("Arbitrary client evaluation before server filtering", doc, StringComparison.Ordinal);
        Assert.Contains("Unsupported", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Matrix_documents_Include_unmapped_nav_prop_throws()
    {
        var doc = Doc();
        Assert.Contains("NormUnsupportedFeatureException", doc, StringComparison.Ordinal);
        Assert.Contains("`Include`, `ThenInclude` | Supported", doc, StringComparison.Ordinal);
    }
}
