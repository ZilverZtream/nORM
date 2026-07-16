using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for decimal aggregate precision (Query/LINQ matrix cell: aggregates x decimal operand).
///
/// Decimal Sum / Average / Min / Max match .NET <see cref="Enumerable"/> at FULL decimal precision on
/// SQLite: SUM and AVG run through registered aggregate functions that accumulate in
/// <see cref="decimal"/> and return canonical decimal text, and MIN / MAX compare through the
/// NORM_DECIMAL collation so they pick the true extreme and always return an ACTUAL stored value.
/// (The previous REAL coercion silently lost precision beyond ~16 significant digits and could
/// return a Min/Max value that did not exist in the data.) Grouped (g.Sum) and filtered
/// (g.Sum(..) over Where) aggregates and navigation aggregates route through the same hook.
/// Server providers aggregate their native DECIMAL exactly. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DecimalAggregatePrecisionContractTests
{
    [Table("DecAggContract")]
    private sealed class D
    {
        [Key] public int Id { get; set; }
        public int Grp { get; set; }
        public decimal Val { get; set; }
    }

    // The 17th-significant-digit pair: both round to exactly 1.0 as double, so any REAL-coerced
    // aggregate merges them. Plus a 26-digit magnitude where double drops the low-order +1.
    private static readonly (int Id, int Grp, decimal Val)[] Rows =
    {
        (1, 1, 1.00000000000000005m),
        (2, 1, 1.00000000000000006m),
        (3, 2, 79000000000000000000000000m),
        (4, 2, 1m),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE DecAggContract (Id INTEGER PRIMARY KEY, Grp INTEGER NOT NULL, Val TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, grp, val) in Rows) await ctx.InsertAsync(new D { Id = id, Grp = grp, Val = val });
        return ctx;
    }

    [Fact]
    public async Task Sum_average_min_max_decimal_match_dotnet_at_full_precision()
    {
        using var ctx = await SeedAsync();
        var g1 = Rows.Where(r => r.Grp == 1).Select(r => r.Val).ToArray();
        var q = ((INormQueryable<D>)ctx.Query<D>()).AsNoTracking().Where(d => d.Grp == 1);

        Assert.Equal(g1.Sum(), q.Sum(d => d.Val));           // 2.00000000000000011 (not 2)
        Assert.Equal(g1.Average(), q.Average(d => d.Val));   // 1.000000000000000055
        Assert.Equal(g1.Min(), q.Min(d => d.Val));           // an ACTUAL value, ...05
        Assert.Equal(g1.Max(), q.Max(d => d.Val));           // an ACTUAL value, ...06
    }

    [Fact]
    public async Task Sum_decimal_preserves_low_order_digits_at_large_magnitude()
    {
        using var ctx = await SeedAsync();
        var g2 = Rows.Where(r => r.Grp == 2).Select(r => r.Val).ToArray();
        var q = ((INormQueryable<D>)ctx.Query<D>()).AsNoTracking().Where(d => d.Grp == 2);

        Assert.Equal(g2.Sum(), q.Sum(d => d.Val));           // 79000000000000000000000001 (keeps the +1)
    }

    [Fact]
    public async Task Grouped_decimal_aggregates_match_dotnet_at_full_precision()
    {
        using var ctx = await SeedAsync();

        var norm = ((INormQueryable<D>)ctx.Query<D>()).AsNoTracking()
            .GroupBy(d => d.Grp)
            .Select(g => new { Grp = g.Key, Sum = g.Sum(x => x.Val), Min = g.Min(x => x.Val), Max = g.Max(x => x.Val) })
            .OrderBy(x => x.Grp)
            .ToList();

        var oracle = Rows
            .GroupBy(r => r.Grp)
            .Select(g => new { Grp = g.Key, Sum = g.Sum(x => x.Val), Min = g.Min(x => x.Val), Max = g.Max(x => x.Val) })
            .OrderBy(x => x.Grp)
            .ToList();

        Assert.Equal(oracle.Count, norm.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].Grp, norm[i].Grp);
            Assert.Equal(oracle[i].Sum, norm[i].Sum);
            Assert.Equal(oracle[i].Min, norm[i].Min);
            Assert.Equal(oracle[i].Max, norm[i].Max);
        }
    }

    [Fact]
    public void Empty_decimal_aggregate_semantics_unchanged()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE DecAggContract (Id INTEGER PRIMARY KEY, Grp INTEGER NOT NULL, Val TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = (INormQueryable<D>)ctx.Query<D>();

        // Empty set: Sum(decimal) = 0 per Enumerable; Min/Max throw; Average throws.
        Assert.Equal(0m, q.AsNoTracking().Sum(d => d.Val));
        Assert.Throws<InvalidOperationException>(() => q.AsNoTracking().Min(d => d.Val));
        Assert.Throws<InvalidOperationException>(() => q.AsNoTracking().Average(d => d.Val));
    }
}
