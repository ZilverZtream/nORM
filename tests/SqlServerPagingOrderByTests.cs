using System;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SG-1: Verifies that SqlServerProvider.ApplyPaging correctly detects ORDER BY in a
/// case-insensitive manner. Previously the check was case-sensitive (Contains("ORDER BY"))
/// so lowercase or mixed-case ORDER BY clauses were not detected and a duplicate
/// ORDER BY (SELECT NULL) was appended.
/// </summary>
public class SqlServerPagingOrderByTests
{
    private static SqlServerProvider Provider => new SqlServerProvider();

    private static string ApplyPaging(string existingSql, int? limit = 10, int? offset = 0)
    {
        using var sb = new OptimizedSqlBuilder();
        sb.Append(existingSql);
        Provider.ApplyPaging(sb, limit, offset, "@limit", "@offset");
        return sb.ToString();
    }

    // ─── SG-1: Uppercase ORDER BY — should NOT add (SELECT NULL) ─────────────

    [Fact]
    public void ApplyPaging_UppercaseOrderBy_DoesNotDuplicate()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget] ORDER BY Id");
        // Must contain exactly one ORDER BY phrase.
        var count = CountOccurrences(sql, "ORDER BY");
        Assert.Equal(1, count);
    }

    // ─── SG-1: Lowercase order by — should NOT add (SELECT NULL) ─────────────

    [Fact]
    public void ApplyPaging_LowercaseOrderBy_DoesNotDuplicate()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget] order by Id");
        // Previously the fix was missing: lowercase "order by" was not detected,
        // so (SELECT NULL) was added after OFFSET, creating invalid SQL.
        var lowerCount = CountOccurrences(sql.ToLowerInvariant(), "order by");
        Assert.Equal(1, lowerCount);
    }

    // ─── SG-1: Mixed-case Order By — should NOT add (SELECT NULL) ────────────

    [Fact]
    public void ApplyPaging_MixedCaseOrderBy_DoesNotDuplicate()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget] Order By Id");
        var count = CountOccurrences(sql.ToLowerInvariant(), "order by");
        Assert.Equal(1, count);
    }

    // ─── SG-1: No ORDER BY present — MUST add (SELECT NULL) ──────────────────

    [Fact]
    public void ApplyPaging_NoOrderBy_AddsSelectNull()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget]");
        Assert.Contains("ORDER BY (SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── SG-1: OFFSET + FETCH syntax is present regardless of ORDER BY case ──

    [Fact]
    public void ApplyPaging_AnyOrderByCasing_ContainsOffsetFetch()
    {
        foreach (var orderBy in new[] { "ORDER BY Id", "order by Id", "Order By Id" })
        {
            var sql = ApplyPaging($"SELECT Id FROM [Widget] {orderBy}");
            Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("ROWS", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static int CountOccurrences(string source, string pattern)
    {
        int count = 0, index = 0;
        while ((index = source.IndexOf(pattern, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += pattern.Length;
        }
        return count;
    }
}
