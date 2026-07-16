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
/// Contract for Join / GroupJoin key-equality semantics per key type (Query/LINQ matrix cell).
///
/// Join keys compare like .NET's default equality comparer for every probed key type:
///
///  * nullable keys: NULL never matches NULL. SQL's <c>NULL = NULL</c> is not true, and .NET's
///    <c>Lookup.CreateForJoin</c> also skips null keys, so both sides agree - a null-key outer row
///    joins nothing. In <c>GroupJoin</c> the null-key outer row still appears, with an EMPTY inner
///    group, exactly like Enumerable.
///  * string keys join case-SENSITIVELY (ordinal): "a" joins "a" but not "A".
///  * decimal keys join at FULL precision: keys agreeing to 16 significant digits and differing at
///    the 17th match only their exact counterpart (no approximate merge).
///  * composite anonymous keys compare component-wise; a null component blocks the match like .NET.
///
/// See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class JoinKeySemanticsContractTests
{
    [Table("JoinKeyL")]
    private sealed class L
    {
        [Key] public int Id { get; set; }
        public int? NKey { get; set; }
        public string SKey { get; set; } = "";
        public decimal DKey { get; set; }
    }

    [Table("JoinKeyR")]
    private sealed class R
    {
        [Key] public int Id { get; set; }
        public int? NKey { get; set; }
        public string SKey { get; set; } = "";
        public decimal DKey { get; set; }
    }

    private static readonly (int Id, int? N, string S, decimal D)[] Ls =
    {
        (1, 1,    "a", 1.00000000000000005m),
        (2, null, "A", 1.00000000000000006m),
        (3, 2,    "b", 2m),
    };
    private static readonly (int Id, int? N, string S, decimal D)[] Rs =
    {
        (10, 1,    "a", 1.00000000000000005m),
        (20, null, "a", 1.00000000000000006m),
        (30, 3,    "A", 2m),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE JoinKeyL (Id INTEGER PRIMARY KEY, NKey INTEGER, SKey TEXT NOT NULL, DKey TEXT NOT NULL);" +
                "CREATE TABLE JoinKeyR (Id INTEGER PRIMARY KEY, NKey INTEGER, SKey TEXT NOT NULL, DKey TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var l in Ls) await ctx.InsertAsync(new L { Id = l.Id, NKey = l.N, SKey = l.S, DKey = l.D });
        foreach (var r in Rs) await ctx.InsertAsync(new R { Id = r.Id, NKey = r.N, SKey = r.S, DKey = r.D });
        return ctx;
    }

    [Fact]
    public async Task Join_nullable_key_never_matches_null_to_null()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<L>)ctx.Query<L>()).AsNoTracking()
            .Join(((INormQueryable<R>)ctx.Query<R>()).AsNoTracking(),
                l => l.NKey, r => r.NKey, (l, r) => new { l.Id, RId = r.Id })
            .ToList().OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        var oracle = Ls.Join(Rs, l => l.N, r => r.N, (l, r) => new { l.Id, RId = r.Id })
            .OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        Assert.Equal(oracle.Select(x => (x.Id, x.RId)), norm.Select(x => (x.Id, x.RId)));
        Assert.Single(norm);                       // only 1-10; the null pair (2, 20) must NOT match
        Assert.Equal((1, 10), (norm[0].Id, norm[0].RId));
    }

    [Fact]
    public async Task GroupJoin_null_key_outer_appears_with_empty_group()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<L>)ctx.Query<L>()).AsNoTracking()
            .GroupJoin(((INormQueryable<R>)ctx.Query<R>()).AsNoTracking(),
                l => l.NKey, r => r.NKey, (l, g) => new { l.Id, N = g.Count() })
            .ToList().OrderBy(x => x.Id).ToList();

        var oracle = Ls.GroupJoin(Rs, l => l.N, r => r.N, (l, g) => new { l.Id, N = g.Count() })
            .OrderBy(x => x.Id).ToList();

        Assert.Equal(oracle.Select(x => (x.Id, x.N)), norm.Select(x => (x.Id, x.N)));
        Assert.Contains(norm, x => x.Id == 2 && x.N == 0);   // the null-key outer row survives, empty
    }

    [Fact]
    public async Task Join_string_key_is_ordinal_case_sensitive()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<L>)ctx.Query<L>()).AsNoTracking()
            .Join(((INormQueryable<R>)ctx.Query<R>()).AsNoTracking(),
                l => l.SKey, r => r.SKey, (l, r) => new { l.Id, RId = r.Id })
            .ToList().OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        var oracle = Ls.Join(Rs, l => l.S, r => r.S, (l, r) => new { l.Id, RId = r.Id })
            .OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        Assert.Equal(oracle.Select(x => (x.Id, x.RId)), norm.Select(x => (x.Id, x.RId)));
        // "a"(1) joins both "a" rows (10, 20); "A"(2) joins only "A"(30); "b"(3) joins nothing.
        Assert.Equal(new[] { (1, 10), (1, 20), (2, 30) }, norm.Select(x => (x.Id, x.RId)));
    }

    [Fact]
    public async Task Join_decimal_key_matches_at_full_precision()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<L>)ctx.Query<L>()).AsNoTracking()
            .Join(((INormQueryable<R>)ctx.Query<R>()).AsNoTracking(),
                l => l.DKey, r => r.DKey, (l, r) => new { l.Id, RId = r.Id })
            .ToList().OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        var oracle = Ls.Join(Rs, l => l.D, r => r.D, (l, r) => new { l.Id, RId = r.Id })
            .OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        Assert.Equal(oracle.Select(x => (x.Id, x.RId)), norm.Select(x => (x.Id, x.RId)));
        // ...05 joins only ...05, ...06 only ...06 - an approximate (REAL) key would cross-match all four.
        Assert.Equal(new[] { (1, 10), (2, 20), (3, 30) }, norm.Select(x => (x.Id, x.RId)));
    }

    [Fact]
    public async Task Join_composite_key_compares_component_wise_with_null_blocking()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<L>)ctx.Query<L>()).AsNoTracking()
            .Join(((INormQueryable<R>)ctx.Query<R>()).AsNoTracking(),
                l => new { l.SKey, l.NKey }, r => new { r.SKey, r.NKey }, (l, r) => new { l.Id, RId = r.Id })
            .ToList().OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        var oracle = Ls.Join(Rs,
                l => new { SKey = l.S, NKey = l.N }, r => new { SKey = r.S, NKey = r.N },
                (l, r) => new { l.Id, RId = r.Id })
            .OrderBy(x => x.Id).ThenBy(x => x.RId).ToList();

        Assert.Equal(oracle.Select(x => (x.Id, x.RId)), norm.Select(x => (x.Id, x.RId)));
        Assert.Single(norm);   // only ("a",1)-("a",1); null components block every other pair
    }
}
