#nullable enable

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// GroupBy over a set operation (Union/Concat/Intersect/Except) must aggregate the combined rows, matching
/// LINQ-to-Objects. A bare compound SELECT cannot resolve the group key, so nORM wraps it as a derived table;
/// this verifies the wrap produces the correct grouped aggregates rather than throwing or aggregating one arm.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SetOpGroupByTests
{
    [Table("SgRow")]
    public sealed class SgRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly SgRow[] Data =
    {
        new() { Id = 1, A = 1, B = 10 },
        new() { Id = 2, A = 1, B = 20 },
        new() { Id = 3, A = 2, B = 30 },
        new() { Id = 4, A = 3, B = 40 },
    };

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SgRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { OnModelCreating = mb => mb.Entity<SgRow>().HasKey(x => x.Id) }, ownsConnection: true);
        foreach (var r in Data) { ctx.Add(new SgRow { Id = r.Id, A = r.A, B = r.B }); }
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return ctx;
    }

    private static (int Key, int Val)[] Sorted(IEnumerable<(int, int)> pairs) =>
        pairs.OrderBy(p => p.Item1).ThenBy(p => p.Item2).ToArray();

    [Fact]
    public void Concat_then_groupby_count_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Concat(ctx.Query<SgRow>())
            .GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() }).ToList()
            .Select(x => (x.Key, x.C));
        var oracle = Data.Concat(Data)
            .GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .Select(x => (x.Key, x.C));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }

    [Fact]
    public void Union_then_groupby_count_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Union(ctx.Query<SgRow>())
            .GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() }).ToList()
            .Select(x => (x.Key, x.C));
        var oracle = Data.Union(Data)
            .GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .Select(x => (x.Key, x.C));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }

    [Fact]
    public void Concat_then_groupby_sum_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Concat(ctx.Query<SgRow>())
            .GroupBy(x => x.A).Select(g => new { g.Key, V = g.Sum(r => r.B) }).ToList()
            .Select(x => (x.Key, x.V));
        var oracle = Data.Concat(Data)
            .GroupBy(x => x.A).Select(g => new { g.Key, V = g.Sum(r => r.B) })
            .Select(x => (x.Key, x.V));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }

    [Fact]
    public void Concat_then_groupby_having_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Concat(ctx.Query<SgRow>())
            .GroupBy(x => x.A).Where(g => g.Count() > 2).Select(g => new { g.Key, C = g.Count() }).ToList()
            .Select(x => (x.Key, x.C));
        var oracle = Data.Concat(Data)
            .GroupBy(x => x.A).Where(g => g.Count() > 2).Select(g => new { g.Key, C = g.Count() })
            .Select(x => (x.Key, x.C));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }

    [Fact]
    public void Plain_groupby_having_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>()
            .GroupBy(x => x.A).Where(g => g.Sum(r => r.B) >= 30).Select(g => new { g.Key, V = g.Sum(r => r.B) }).ToList()
            .Select(x => (x.Key, x.V));
        var oracle = Data
            .GroupBy(x => x.A).Where(g => g.Sum(r => r.B) >= 30).Select(g => new { g.Key, V = g.Sum(r => r.B) })
            .Select(x => (x.Key, x.V));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }

    [Fact]
    public void Concat_then_groupby_composite_key_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Concat(ctx.Query<SgRow>())
            .GroupBy(x => new { x.A, x.B }).Select(g => new { g.Key.A, g.Key.B, C = g.Count() }).ToList()
            .Select(x => (x.A, x.B, x.C));
        var oracle = Data.Concat(Data)
            .GroupBy(x => new { x.A, x.B }).Select(g => new { g.Key.A, g.Key.B, C = g.Count() })
            .Select(x => (x.A, x.B, x.C));
        Assert.Equal(oracle.OrderBy(x => x.A).ThenBy(x => x.B).ToArray(), norm.OrderBy(x => x.A).ThenBy(x => x.B).ToArray());
    }

    [Fact]
    public void Concat_then_groupby_result_selector_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Concat(ctx.Query<SgRow>())
            .GroupBy(x => x.A, (k, g) => new { Key = k, C = g.Count() }).ToList()
            .Select(x => (x.Key, x.C));
        var oracle = Data.Concat(Data)
            .GroupBy(x => x.A, (k, g) => new { Key = k, C = g.Count() })
            .Select(x => (x.Key, x.C));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }

    [Fact]
    public void Filtered_concat_then_groupby_count_matches_linq()
    {
        using var ctx = NewCtx();
        var norm = ctx.Query<SgRow>().Where(r => r.A >= 1).Concat(ctx.Query<SgRow>().Where(r => r.B > 25))
            .GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() }).ToList()
            .Select(x => (x.Key, x.C));
        var oracle = Data.Where(r => r.A >= 1).Concat(Data.Where(r => r.B > 25))
            .GroupBy(x => x.A).Select(g => new { g.Key, C = g.Count() })
            .Select(x => (x.Key, x.C));
        Assert.Equal(Sorted(oracle), Sorted(norm));
    }
}
