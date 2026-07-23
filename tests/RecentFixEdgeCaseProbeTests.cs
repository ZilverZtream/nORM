#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Adversarial edge cases for two recent fixes: the spine-walk that wraps a set-op/Distinct source before a
/// GroupBy, and the DefaultIfEmpty-to-COALESCE lowering for a correlated value aggregate. Probes the corners
/// most likely to have been missed — Distinct/Where between Select and GroupBy, a double Distinct, a decimal
/// fallback, and a nullable aggregate column — against a LINQ-to-Objects oracle.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class RecentFixEdgeCaseProbeTests
{
    [Table("RfeRow")]
    public sealed class RfeRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public decimal D { get; set; }
        public int? N { get; set; }
    }

    private static readonly RfeRow[] Data =
    {
        new() { Id = 1, A = 1, B = 10, D = 1.5m,  N = 5 },
        new() { Id = 2, A = 1, B = 20, D = 2.5m,  N = null },
        new() { Id = 3, A = 2, B = 30, D = 3.5m,  N = 15 },
        new() { Id = 4, A = 2, B = 10, D = 1.5m,  N = null },
        new() { Id = 5, A = 3, B = 40, D = 4.25m, N = 25 },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE RfeRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, D TEXT NOT NULL, N INTEGER NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<RfeRow>().HasKey(x => x.Id) }, ownsConnection: true);
        foreach (var r in Data) ctx.Add(new RfeRow { Id = r.Id, A = r.A, B = r.B, D = r.D, N = r.N });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    private static IQueryable<RfeRow> Q(DbContext ctx) => ctx.Query<RfeRow>();

    // ── set-op/Distinct spine-walk edges ──────────────────────────────────────────────────────────

    [Fact]
    public void Distinct_then_select_then_groupby()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Distinct().Select(x => new { x.A, x.B }).GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.Distinct().Select(x => new { x.A, x.B }).GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Where_then_distinct_then_groupby()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Where(x => x.B >= 10).Distinct().GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.Where(x => x.B >= 10).Distinct().GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Concat_double_distinct_then_groupby()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Concat(Q(ctx)).Distinct().Distinct().GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.Concat(Data).Distinct().Distinct().GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Setop_distinct_groupby_sum_over_computed()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Concat(Q(ctx)).Distinct().GroupBy(x => x.A).Select(g => new { g.Key, S = g.Sum(x => x.B * 2) })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.S)).ToList();
        var oracle = Data.Concat(Data).Distinct().GroupBy(x => x.A).Select(g => new { g.Key, S = g.Sum(x => x.B * 2) })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.S)).ToList();
        Assert.Equal(oracle, norm);
    }

    // ── DefaultIfEmpty COALESCE edges ─────────────────────────────────────────────────────────────

    [Fact]
    public void DefaultIfEmpty_decimal_fallback()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<RfeRow>().OrderBy(x => x.Id)
            .Select(x => new { x.Id, M = ctx.Query<RfeRow>().Where(o => o.A == x.A + 100).Select(o => o.D).DefaultIfEmpty(-1.5m).Max() })
            .ToList().Select(x => (x.Id, x.M)).ToList();
        var oracle = Data.OrderBy(x => x.Id)
            .Select(x => new { x.Id, M = Data.Where(o => o.A == x.A + 100).Select(o => o.D).DefaultIfEmpty(-1.5m).Max() })
            .Select(x => (x.Id, x.M)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void DefaultIfEmpty_over_nullable_column_nonempty()
    {
        using var ctx = NewCtx();
        // Max over a nullable column with a fallback; the correlated set is non-empty (same A), and SQL MAX
        // ignores NULLs, matching Enumerable.Max over int? which also skips nulls... but DefaultIfEmpty of a
        // non-empty set is a pass-through, so compare directly.
        var norm = ctx.Query<RfeRow>().OrderBy(x => x.Id)
            .Select(x => new { x.Id, M = ctx.Query<RfeRow>().Where(o => o.A == x.A).Select(o => o.N ?? 0).DefaultIfEmpty(-1).Max() })
            .ToList().Select(x => (x.Id, x.M)).ToList();
        var oracle = Data.OrderBy(x => x.Id)
            .Select(x => new { x.Id, M = Data.Where(o => o.A == x.A).Select(o => o.N ?? 0).DefaultIfEmpty(-1).Max() })
            .Select(x => (x.Id, x.M)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void DefaultIfEmpty_min_empty_and_nonempty_mixed()
    {
        using var ctx = NewCtx();
        // A correlates so that A==1 and A==2 are non-empty but A==3 minus-nothing; use A-1 so A==1 -> A==0 empty.
        var norm = ctx.Query<RfeRow>().OrderBy(x => x.Id)
            .Select(x => new { x.Id, M = ctx.Query<RfeRow>().Where(o => o.A == x.A - 1).Select(o => o.B).DefaultIfEmpty(999).Min() })
            .ToList().Select(x => (x.Id, x.M)).ToList();
        var oracle = Data.OrderBy(x => x.Id)
            .Select(x => new { x.Id, M = Data.Where(o => o.A == x.A - 1).Select(o => o.B).DefaultIfEmpty(999).Min() })
            .Select(x => (x.Id, x.M)).ToList();
        Assert.Equal(oracle, norm);
    }
}
