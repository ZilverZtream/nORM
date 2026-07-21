using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Oracle-compared coverage for nullable propagation: nullable arithmetic (which yields NULL when an
/// operand is null), the null-coalescing operator, nullable casts, HasValue/Value, and null
/// comparisons in projections, predicates, and aggregates. Three-valued SQL logic can silently
/// diverge from C# nullable semantics here.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NullablePropagationTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NppRow")]
    private sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int A { get; set; }
        public int? N { get; set; }   // nullable — every 3rd row is null
        public int? M { get; set; }   // nullable — every 4th row is null
    }

    private static readonly Row[] Rows = Enumerable.Range(1, 24).Select(i => new Row
    {
        Id = i,
        A = i,
        N = i % 3 == 0 ? (int?)null : i * 2,
        M = i % 4 == 0 ? (int?)null : i + 100,
    }).ToArray();

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NppRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, N INTEGER NULL, M INTEGER NULL);";
            foreach (var r in Rows)
                cmd.CommandText += $"INSERT INTO NppRow VALUES ({r.Id},{r.A},{(r.N.HasValue ? r.N.Value.ToString() : "NULL")},{(r.M.HasValue ? r.M.Value.ToString() : "NULL")});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Coalesce_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.N ?? -1).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.N ?? -1).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Coalesce_chain_projection_matches_linq()
    {
        // N ?? M ?? 0 — chained coalesce over two nullable columns.
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.N ?? r.M ?? 0).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.N ?? r.M ?? 0).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Nullable_arithmetic_projection_matches_linq()
    {
        // N + A is null when N is null (nullable arithmetic propagates null). Compare via coalesce
        // so the projected sequence is int (not int?) and rows line up.
        var expected = Rows.OrderBy(r => r.Id).Select(r => (r.N + r.A) ?? -999).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (r.N + r.A) ?? -999).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Two_nullable_arithmetic_matches_linq()
    {
        // N + M is null when EITHER is null.
        var expected = Rows.OrderBy(r => r.Id).Select(r => (r.N + r.M) ?? -1).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => (r.N + r.M) ?? -1).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Hasvalue_and_value_projection_matches_linq()
    {
        var expected = Rows.OrderBy(r => r.Id).Select(r => r.N.HasValue ? r.N.Value : 0).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.Id).Select(r => r.N.HasValue ? r.N.Value : 0).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Null_comparison_predicates_match_linq()
    {
        using var ctx = Ctx();
        Assert.Equal(
            Rows.Where(r => r.N == null).Select(r => r.Id).OrderBy(v => v).ToList(),
            ctx.Query<Row>().Where(r => r.N == null).Select(r => r.Id).OrderBy(v => v).ToList());
        Assert.Equal(
            Rows.Where(r => r.N != null && r.N > 20).Select(r => r.Id).OrderBy(v => v).ToList(),
            ctx.Query<Row>().Where(r => r.N != null && r.N > 20).Select(r => r.Id).OrderBy(v => v).ToList());
    }

    [Fact]
    public void Coalesce_in_predicate_matches_linq()
    {
        // (N ?? 0) > 10 — coalesce inside a predicate; null rows use the fallback.
        var expected = Rows.Where(r => (r.N ?? 0) > 10).Select(r => r.Id).OrderBy(v => v).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Where(r => (r.N ?? 0) > 10).Select(r => r.Id).OrderBy(v => v).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Sum_of_coalesced_nullable_matches_linq()
    {
        // Sum(N ?? 0) — aggregate over a coalesced nullable.
        var expected = Rows.Sum(r => r.N ?? 0);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Sum(r => r.N ?? 0);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Nullable_ordering_with_coalesce_key_matches_linq()
    {
        // ORDER BY (N ?? int.MaxValue) — nulls sort last.
        var expected = Rows.OrderBy(r => r.N ?? int.MaxValue).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().OrderBy(r => r.N ?? int.MaxValue).ThenBy(r => r.Id).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }
}
