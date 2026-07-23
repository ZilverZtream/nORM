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
/// Probes advanced query shapes against a LINQ-to-Objects oracle to find any that nORM cannot translate
/// (a NormUnsupportedFeatureException surfaces as a test failure) or translates to the wrong result. Shapes
/// that pass become regression keepers; a shape that fails is a feature gap to implement.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class AdvancedQueryShapeProbeTests
{
    [Table("AqRow")]
    public sealed class AqRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly AqRow[] Data =
    {
        new() { Id = 1, A = 1, B = 10 },
        new() { Id = 2, A = 1, B = 20 },
        new() { Id = 3, A = 2, B = 30 },
        new() { Id = 4, A = 2, B = 5 },
        new() { Id = 5, A = 3, B = 40 },
        new() { Id = 6, A = 3, B = 40 },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AqRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<AqRow>().HasKey(x => x.Id) }, ownsConnection: true);
        foreach (var r in Data) ctx.Add(new AqRow { Id = r.Id, A = r.A, B = r.B });
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    private static IQueryable<AqRow> Q(DbContext ctx) => ctx.Query<AqRow>();

    [Fact]
    public void Nested_set_op_union_of_union()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Where(r => r.A == 1).Union(Q(ctx).Where(r => r.A == 2)).Union(Q(ctx).Where(r => r.A == 3))
            .Select(r => r.Id).OrderBy(x => x).ToList();
        var oracle = Data.Where(r => r.A == 1).Union(Data.Where(r => r.A == 2)).Union(Data.Where(r => r.A == 3))
            .Select(r => r.Id).OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Set_op_over_scalar_projections()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Select(r => r.A).Union(Q(ctx).Select(r => r.B)).OrderBy(x => x).ToList();
        var oracle = Data.Select(r => r.A).Union(Data.Select(r => r.B)).OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Top_n_groups_by_count()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() })
            .OrderByDescending(x => x.C).ThenBy(x => x.Key).Take(2).ToList()
            .Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() })
            .OrderByDescending(x => x.C).ThenBy(x => x.Key).Take(2)
            .Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_having_orderby_take()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(r => r.A).Where(g => g.Sum(x => x.B) >= 25)
            .Select(g => new { g.Key, S = g.Sum(x => x.B) }).OrderBy(x => x.S).Take(2).ToList()
            .Select(x => (x.Key, x.S)).ToList();
        var oracle = Data.GroupBy(r => r.A).Where(g => g.Sum(x => x.B) >= 25)
            .Select(g => new { g.Key, S = g.Sum(x => x.B) }).OrderBy(x => x.S).Take(2)
            .Select(x => (x.Key, x.S)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Distinct_projection_then_count()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Select(r => r.A).Distinct().Count();
        var oracle = Data.Select(r => r.A).Distinct().Count();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Set_op_then_skip_take()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Concat(Q(ctx)).OrderBy(r => r.Id).Select(r => r.Id).Skip(3).Take(4).ToList();
        var oracle = Data.Concat(Data).OrderBy(r => r.Id).Select(r => r.Id).Skip(3).Take(4).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void GroupBy_multiple_aggregates()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).GroupBy(r => r.A)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.B), Mn = g.Min(x => x.B), Mx = g.Max(x => x.B) })
            .OrderBy(x => x.Key).ToList()
            .Select(x => (x.Key, x.C, x.S, x.Mn, x.Mx)).ToList();
        var oracle = Data.GroupBy(r => r.A)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.B), Mn = g.Min(x => x.B), Mx = g.Max(x => x.B) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.C, x.S, x.Mn, x.Mx)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Set_op_distinct_then_groupby()
    {
        using var ctx = NewCtx();
        var norm = Q(ctx).Concat(Q(ctx)).Distinct().GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).ToList().Select(x => (x.Key, x.C)).ToList();
        var oracle = Data.Concat(Data).Distinct().GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() })
            .OrderBy(x => x.Key).Select(x => (x.Key, x.C)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Correlated_count_subquery_in_projection()
    {
        using var ctx = NewCtx();
        // For each row, count how many rows share its A group (correlated subquery in the projection).
        // The subquery root must be a direct ctx.Query<T>() inside the lambda — that is the form the
        // projection lowerer recognizes as a correlated scalar subquery.
        var norm = ctx.Query<AqRow>().Select(r => new { r.Id, Peers = ctx.Query<AqRow>().Count(o => o.A == r.A) })
            .OrderBy(x => x.Id).ToList().Select(x => (x.Id, x.Peers)).ToList();
        var oracle = Data.Select(r => new { r.Id, Peers = Data.Count(o => o.A == r.A) })
            .OrderBy(x => x.Id).Select(x => (x.Id, x.Peers)).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public void Group_key_orderby_then_first_per_shape()
    {
        using var ctx = NewCtx();
        // Distinct A values ordered, then for each take the max B — a grouped top value.
        var norm = Q(ctx).GroupBy(r => r.A).Select(g => new { g.Key, Top = g.Max(x => x.B) })
            .Where(x => x.Top > 10).OrderByDescending(x => x.Top).ToList()
            .Select(x => (x.Key, x.Top)).ToList();
        var oracle = Data.GroupBy(r => r.A).Select(g => new { g.Key, Top = g.Max(x => x.B) })
            .Where(x => x.Top > 10).OrderByDescending(x => x.Top)
            .Select(x => (x.Key, x.Top)).ToList();
        Assert.Equal(oracle, norm);
    }
}
