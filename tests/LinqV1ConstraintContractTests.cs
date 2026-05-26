using System;
using System.IO;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the LINQ matrix in <c>docs/linq-support.md</c> as the v1 contract for query-shape
/// support. The matrix must not silently drift - either by claiming shapes that aren't
/// implemented or by dropping the documented Constrained/Supported labels readers depend on.
///
/// Several constrained shapes (composite-key dependent includes, streaming IGrouping, batched
/// GroupJoin correlated subcollections, CROSS APPLY / LATERAL / UNNEST SelectMany expansion)
/// throw <c>NormUnsupportedFeatureException</c> in v1. The deferred translator work for those
/// shapes is tracked outside this contract; this test only enforces that the documented
/// behavior stays consistent with what callers can rely on.
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
    [InlineData("Set operations: `Union`, `Intersect`, `Except`", "Supported")]
    [InlineData("`AsSplitQuery`, `AsNoTracking`, caching, temporal `AsOf`", "Supported")]
    public void Matrix_marks_v1_supported_shape(string feature, string status)
    {
        var doc = Doc();
        var probe = $"| {feature} | {status} |";
        Assert.Contains(probe, doc, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Group joins", "Supported")]
    [InlineData("`AsAsyncEnumerable`", "Supported")]
    public void Matrix_marks_v1_supported_shape_promoted(string feature, string status)
    {
        var doc = Doc();
        var probe = $"| {feature} | {status} |";
        Assert.Contains(probe, doc, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("`GroupBy`", "Constrained")]
    [InlineData("`SelectMany`", "Constrained")]
    [InlineData("`Include`, `ThenInclude`", "Constrained")]
    public void Matrix_marks_v1_constrained_shape(string feature, string status)
    {
        var doc = Doc();
        var probe = $"| {feature} | {status} |";
        Assert.Contains(probe, doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Matrix_documents_deferred_Include_shape()
    {
        var doc = Doc();
        Assert.Contains("Composite-key dependent includes", doc, StringComparison.Ordinal);
        Assert.Contains("NormUnsupportedFeatureException", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void Matrix_documents_unsupported_client_evaluation_shape()
    {
        var doc = Doc();
        Assert.Contains("Arbitrary client evaluation before server filtering", doc, StringComparison.Ordinal);
        Assert.Contains("Unsupported", doc, StringComparison.Ordinal);
    }
}
