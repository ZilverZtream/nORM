using System;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that SqlServerProvider.ApplyPaging correctly detects ORDER BY in a
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

    // ─── Uppercase ORDER BY — should NOT add (SELECT NULL) ─────────────

    [Fact]
    public void ApplyPaging_UppercaseOrderBy_DoesNotDuplicate()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget] ORDER BY Id");
        // Must contain exactly one ORDER BY phrase.
        var count = CountOccurrences(sql, "ORDER BY");
        Assert.Equal(1, count);
    }

    // ─── Lowercase order by — should NOT add (SELECT NULL) ─────────────

    [Fact]
    public void ApplyPaging_LowercaseOrderBy_DoesNotDuplicate()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget] order by Id");
        // Previously the fix was missing: lowercase "order by" was not detected,
        // so (SELECT NULL) was added after OFFSET, creating invalid SQL.
        var lowerCount = CountOccurrences(sql.ToLowerInvariant(), "order by");
        Assert.Equal(1, lowerCount);
    }

    // ─── Mixed-case Order By — should NOT add (SELECT NULL) ────────────

    [Fact]
    public void ApplyPaging_MixedCaseOrderBy_DoesNotDuplicate()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget] Order By Id");
        var count = CountOccurrences(sql.ToLowerInvariant(), "order by");
        Assert.Equal(1, count);
    }

    // ─── No ORDER BY present — MUST add (SELECT NULL) ──────────────────

    [Fact]
    public void ApplyPaging_NoOrderBy_AddsSelectNull()
    {
        var sql = ApplyPaging("SELECT Id FROM [Widget]");
        Assert.Contains("ORDER BY (SELECT NULL)", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── OFFSET + FETCH syntax is present regardless of ORDER BY case ──

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

    // ─── S1 adversarial: ORDER BY inside subquery must NOT suppress top-level inject ──

    /// <summary>
    /// Root cause of S1: A plain Contains scan on the full SQL string would find the
    /// "ORDER BY" inside a derived-table subquery and incorrectly skip injecting
    /// "ORDER BY (SELECT NULL)" for the outer OFFSET/FETCH paging.
    /// HasTopLevelOrderBy walks parenthesis depth; only depth-0 ORDER BY counts.
    /// </summary>
    [Fact]
    public void ApplyPaging_SubqueryOrderBy_InFromClause_AddsSelectNull()
    {
        // The ORDER BY is at paren-depth 1 (inside a derived table).
        // The outer SELECT has no ORDER BY → must inject ORDER BY (SELECT NULL).
        const string sql =
            "SELECT t.[Id] FROM (SELECT TOP 10 [Id] FROM [Widget] ORDER BY [Id]) AS t";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_SubqueryOrderBy_InWhereIn_AddsSelectNull()
    {
        // ORDER BY is inside an IN-subquery at paren-depth 1.
        const string sql =
            "SELECT [Id] FROM [Widget] " +
            "WHERE [Id] IN (SELECT TOP 5 [Id] FROM [Archive] ORDER BY [Score] DESC)";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_SubqueryOrderBy_InExistsClause_AddsSelectNull()
    {
        // ORDER BY is inside an EXISTS subquery at paren-depth 1.
        const string sql =
            "SELECT [Id] FROM [Widget] " +
            "WHERE EXISTS (SELECT 1 FROM [Detail] WHERE [Detail].[WidgetId] = [Widget].[Id] ORDER BY [Detail].[Seq])";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_DeeplyNestedSubqueryOrderBy_AddsSelectNull()
    {
        // ORDER BY is at paren-depth 2 — two levels of subquery nesting.
        const string sql =
            "SELECT [Id] FROM [Widget] " +
            "WHERE [Id] IN (SELECT [Id] FROM " +
            "(SELECT TOP 3 [Id] FROM [Inner] ORDER BY [Id]) AS sub)";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_TopLevelOrderBy_AfterSubqueryWithOrderBy_DoesNotDuplicate()
    {
        // Both a subquery ORDER BY (depth 1) AND a top-level ORDER BY (depth 0) exist.
        // HasTopLevelOrderBy should find the depth-0 one → inject nothing.
        const string sql =
            "SELECT t.[Id] FROM " +
            "(SELECT TOP 10 [Id] FROM [Widget] ORDER BY [Score]) AS t " +
            "ORDER BY t.[Id] DESC";

        var result = ApplyPaging(sql);
        // Exactly two ORDER BY occurrences: one from the subquery, one from the top level.
        var count = CountOccurrences(result.ToUpperInvariant(), "ORDER BY");
        Assert.Equal(2, count);
        // And (SELECT NULL) must NOT appear.
        Assert.DoesNotContain("SELECT NULL", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_SubqueryOrderBy_CaseInsensitiveKeyword_AddsSelectNull()
    {
        // Lowercase "order by" inside a subquery — depth-aware scan must be case-insensitive.
        const string sql =
            "SELECT [id] FROM (SELECT TOP 5 [id] FROM [t] order by [id]) sub";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_SubqueryOrderBy_WithAliasedParens_AddsSelectNull()
    {
        // Parenthesized expression in SELECT list that is NOT a subquery —
        // should not confuse the depth counter.
        const string sql =
            "SELECT ([Qty] * [Price]) AS [Total], [Id] FROM [Widget]";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPaging_MultipleSubqueriesWithOrderBy_AddsSelectNull()
    {
        // Two sibling subqueries, each with ORDER BY; outer has none.
        const string sql =
            "SELECT a.[Id], b.[Id] " +
            "FROM (SELECT TOP 5 [Id] FROM [A] ORDER BY [Id]) a " +
            "JOIN (SELECT TOP 5 [Id] FROM [B] ORDER BY [Id] DESC) b ON a.[Id] = b.[Id]";

        var result = ApplyPaging(sql);
        Assert.Contains("ORDER BY (SELECT NULL)", result, StringComparison.OrdinalIgnoreCase);
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
