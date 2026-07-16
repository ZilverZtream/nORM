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
/// Contract for Distinct / set-operation dedup semantics per row shape (Query/LINQ matrix cell).
///
/// Distinct, Union, Intersect, and Except dedup like .NET's default equality comparer:
///
///  * decimal shapes dedup at FULL 28-digit precision - values agreeing to 16 significant digits and
///    differing at the 17th stay DISTINCT (scalar and anonymous-member shapes both), while true
///    duplicates collapse. The dedup key is the provider's exact canonical decimal text, which is also
///    scale-insensitive, and the materialized values keep full precision (no REAL corruption).
///  * string shapes dedup case-SENSITIVELY (ordinal): "a" and "A" are distinct.
///  * anonymous composite shapes dedup component-wise.
///
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DistinctSetOpSemanticsContractTests
{
    [Table("DistinctSetL")]
    private sealed class A
    {
        [Key] public int Id { get; set; }
        public decimal DVal { get; set; }
        public string SVal { get; set; } = "";
    }

    [Table("DistinctSetR")]
    private sealed class B
    {
        [Key] public int Id { get; set; }
        public decimal DVal { get; set; }
        public string SVal { get; set; } = "";
    }

    private static readonly (int Id, decimal D, string S)[] As =
    {
        (1, 1.00000000000000005m, "a"),
        (2, 1.00000000000000006m, "A"),
        (3, 1.00000000000000005m, "a"),   // true duplicate of row 1
        (4, 2m,                   "b"),
    };
    private static readonly (int Id, decimal D, string S)[] Bs =
    {
        (10, 1.00000000000000006m, "A"),
        (11, 3m,                   "c"),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE DistinctSetL (Id INTEGER PRIMARY KEY, DVal TEXT NOT NULL, SVal TEXT NOT NULL);" +
                "CREATE TABLE DistinctSetR (Id INTEGER PRIMARY KEY, DVal TEXT NOT NULL, SVal TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in As) await ctx.InsertAsync(new A { Id = r.Id, DVal = r.D, SVal = r.S });
        foreach (var r in Bs) await ctx.InsertAsync(new B { Id = r.Id, DVal = r.D, SVal = r.S });
        return ctx;
    }

    [Fact]
    public async Task Distinct_decimal_keeps_17th_digit_values_distinct_and_collapses_true_duplicates()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<A>)ctx.Query<A>()).AsNoTracking()
            .Select(a => a.DVal).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = As.Select(r => r.D).Distinct().OrderBy(x => x).ToList();

        Assert.Equal(oracle, norm);
        Assert.Equal(3, norm.Count);   // ...05, ...06, 2 - the pair does NOT merge, the duplicate does
    }

    [Fact]
    public async Task Distinct_string_is_ordinal_case_sensitive()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<A>)ctx.Query<A>()).AsNoTracking()
            .Select(a => a.SVal).Distinct().ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
        var oracle = As.Select(r => r.S).Distinct().OrderBy(x => x, StringComparer.Ordinal).ToList();

        Assert.Equal(oracle, norm);   // A, a, b - case-distinct
    }

    [Fact]
    public async Task Union_intersect_except_decimal_operate_at_full_precision()
    {
        using var ctx = await SeedAsync();
        var qa = ((INormQueryable<A>)ctx.Query<A>()).AsNoTracking().Select(a => a.DVal);
        var qb = ((INormQueryable<B>)ctx.Query<B>()).AsNoTracking().Select(b => b.DVal);

        Assert.Equal(
            As.Select(r => r.D).Union(Bs.Select(r => r.D)).OrderBy(x => x).ToList(),
            qa.Union(qb).ToList().OrderBy(x => x).ToList());

        // Intersect must match ONLY the exact ...06 (an approximate key would also match ...05).
        var ix = qa.Intersect(qb).ToList().OrderBy(x => x).ToList();
        Assert.Equal(As.Select(r => r.D).Intersect(Bs.Select(r => r.D)).OrderBy(x => x).ToList(), ix);
        Assert.Equal(new[] { 1.00000000000000006m }, ix);

        // Except keeps ...05 (removed only if approximately merged with ...06).
        var ex = qa.Except(qb).ToList().OrderBy(x => x).ToList();
        Assert.Equal(As.Select(r => r.D).Except(Bs.Select(r => r.D)).OrderBy(x => x).ToList(), ex);
        Assert.Contains(1.00000000000000005m, ex);
    }

    [Fact]
    public async Task Union_of_anonymous_shapes_preserves_full_decimal_precision_in_materialized_values()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<A>)ctx.Query<A>()).AsNoTracking()
            .Select(a => new { a.DVal, a.SVal })
            .Union(((INormQueryable<B>)ctx.Query<B>()).AsNoTracking().Select(b => new { b.DVal, b.SVal }))
            .ToList().OrderBy(x => x.DVal).ThenBy(x => x.SVal, StringComparer.Ordinal).ToList();

        var oracle = As.Select(r => new { DVal = r.D, SVal = r.S })
            .Union(Bs.Select(r => new { DVal = r.D, SVal = r.S }))
            .OrderBy(x => x.DVal).ThenBy(x => x.SVal, StringComparer.Ordinal).ToList();

        Assert.Equal(oracle.Select(x => (x.DVal, x.SVal)), norm.Select(x => (x.DVal, x.SVal)));
        // The materialized decimals keep the 17th digit - the dedup key must not corrupt the value.
        Assert.Contains(norm, x => x.DVal == 1.00000000000000005m);
        Assert.Contains(norm, x => x.DVal == 1.00000000000000006m);
    }
}
