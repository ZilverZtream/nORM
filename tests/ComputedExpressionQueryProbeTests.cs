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
/// Probes computed-expression, conditional, and null-coalesce query shapes against a LINQ-to-Objects oracle.
/// These are the shapes where a translation defect is silent-wrong (a CASE arm dropped, a null coalesced on the
/// wrong side, an integer computation done in the wrong type) rather than a loud throw, so the oracle compares
/// exact results. Shapes that pass become regression keepers; a mismatch is a correctness bug to fix.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ComputedExpressionQueryProbeTests
{
    [Table("CqRow")]
    public sealed class CqRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public string Name { get; set; } = "";
        public int? N { get; set; }
    }

    private static readonly CqRow[] Data =
    {
        new() { Id = 1, A = 1, B = 10, Name = "alpha", N = 5 },
        new() { Id = 2, A = 1, B = 20, Name = "beta",  N = null },
        new() { Id = 3, A = 2, B = 30, Name = "gamma", N = 15 },
        new() { Id = 4, A = 2, B = 5,  Name = "delta", N = null },
        new() { Id = 5, A = 3, B = 40, Name = "eps",   N = 25 },
        new() { Id = 6, A = 3, B = 40, Name = "zeta",  N = 0 },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Name TEXT NOT NULL, N INTEGER NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<CqRow>().HasKey(x => x.Id) }, ownsConnection: true);
        foreach (var r in Data) ctx.Add(new CqRow { Id = r.Id, A = r.A, B = r.B, Name = r.Name, N = r.N });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    private static IQueryable<CqRow> Q(DbContext ctx) => ctx.Query<CqRow>();

    [Fact]
    public void Sum_over_a_computed_product()
    {
        using var ctx = NewCtx();
        Assert.Equal(Data.Sum(x => x.A * x.B), Q(ctx).Sum(x => x.A * x.B));
    }

    [Fact]
    public void Max_over_a_computed_difference()
    {
        using var ctx = NewCtx();
        Assert.Equal(Data.Max(x => x.A - x.B), Q(ctx).Max(x => x.A - x.B));
    }

    [Fact]
    public void Sum_of_a_conditional_case()
    {
        using var ctx = NewCtx();
        Assert.Equal(Data.Sum(x => x.B > 20 ? x.B : 0), Q(ctx).Sum(x => x.B > 20 ? x.B : 0));
    }

    [Fact]
    public void GroupBy_computed_key_modulo()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.A % 2).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.GroupBy(x => x.A % 2).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_with_aggregate_over_computed()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.A).Select(g => new { g.Key, S = g.Sum(x => x.B * 2) })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.S)).ToList();
        var oracle = Data.GroupBy(x => x.A).Select(g => new { g.Key, S = g.Sum(x => x.B * 2) })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.S)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Conditional_projection_to_string()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.Id).Select(x => x.B > 20 ? "big" : "small").ToList();
        var oracle = Data.OrderBy(x => x.Id).Select(x => x.B > 20 ? "big" : "small").ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Nested_ternary_projection()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.Id).Select(x => x.A == 1 ? "one" : x.A == 2 ? "two" : "other").ToList();
        var oracle = Data.OrderBy(x => x.Id).Select(x => x.A == 1 ? "one" : x.A == 2 ? "two" : "other").ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Coalesce_nullable_projection()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.Id).Select(x => x.N ?? -1).ToList();
        var oracle = Data.OrderBy(x => x.Id).Select(x => x.N ?? -1).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Coalesce_nullable_in_predicate()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Where(x => (x.N ?? 0) > 5).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        var oracle = Data.Where(x => (x.N ?? 0) > 5).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Sum_of_nullable_coalesced()
    {
        using var ctx = NewCtx();
        Assert.Equal(Data.Sum(x => x.N ?? 0), Q(ctx).Sum(x => x.N ?? 0));
    }

    [Fact]
    public void OrderBy_a_computed_key()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).OrderBy(x => x.A * 100 + x.B).Select(x => x.Id).ToList();
        var oracle = Data.OrderBy(x => x.A * 100 + x.B).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Count_with_predicate_on_main_query()
    {
        using var ctx = NewCtx();
        Assert.Equal(Data.Count(x => x.B > 20), Q(ctx).Count(x => x.B > 20));
    }

    [Fact]
    public void GroupBy_computed_key_with_conditional_sum()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(x => x.A).Select(g => new { g.Key, S = g.Sum(x => x.N ?? 0) })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.S)).ToList();
        var oracle = Data.GroupBy(x => x.A).Select(g => new { g.Key, S = g.Sum(x => x.N ?? 0) })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.S)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Average_over_computed_matches_within_tolerance()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Average(x => (double)(x.A + x.B));
        var oracle = Data.Average(x => (double)(x.A + x.B));
        Assert.True(Math.Abs(norm - oracle) < 1e-9, $"avg mismatch: nORM={norm} oracle={oracle}");
    }
}
