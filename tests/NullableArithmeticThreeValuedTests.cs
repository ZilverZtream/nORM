using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Nullable arithmetic and three-valued logic must match LINQ-to-Objects: NULL-propagating
/// arithmetic in predicates (A + B &gt; 10 excludes NULL results) and projections, coalesced
/// arithmetic, and equality/inequality where A != B must INCLUDE rows with exactly one NULL
/// operand (the common silent bug is dropping them via SQL NULL &lt;&gt; x = UNKNOWN).
/// </summary>
[Trait("Category", "Fast")]
public class NullableArithmeticThreeValuedTests
{
    [Table("NatRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int? A { get; set; }
        public int? B { get; set; }
        public int C { get; set; }
    }

    // Cover null in each position and both.
    private static readonly (int Id, int? A, int? B, int C)[] Data =
    {
        (1, 10, 5, 3),
        (2, null, 5, 3),
        (3, 10, null, 3),
        (4, null, null, 3),
        (5, 2, 2, 100),
        (6, -8, 20, 0),
    };

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NatRow (Id INTEGER PRIMARY KEY, A INTEGER NULL, B INTEGER NULL, C INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, a, b, c) in Data)
            {
                using var ins = cn.CreateCommand();
                ins.CommandText = $"INSERT INTO NatRow VALUES ({id}, {(a.HasValue ? a.ToString() : "NULL")}, {(b.HasValue ? b.ToString() : "NULL")}, {c})";
                ins.ExecuteNonQuery();
            }
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static IEnumerable<Row> Oracle() => Data.Select(d => new Row { Id = d.Id, A = d.A, B = d.B, C = d.C });

    [Fact]
    public async Task Nullable_sum_comparison_excludes_null_results()
    {
        await using var ctx = Make();
        // A + B > 10: rows where either is null -> NULL -> UNKNOWN -> excluded (matches C# null > 10 = false).
        var got = (await ctx.Query<Row>().Where(r => r.A + r.B > 10)
            .Select(r => new { r.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().Where(r => r.A + r.B > 10).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Coalesced_arithmetic_predicate()
    {
        await using var ctx = Make();
        // (A ?? 0) + (B ?? 0) + C >= 15
        var got = (await ctx.Query<Row>().Where(r => (r.A ?? 0) + (r.B ?? 0) + r.C >= 15)
            .Select(r => new { r.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var oracle = Oracle().Where(r => (r.A ?? 0) + (r.B ?? 0) + r.C >= 15).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(oracle, got);
    }

    [Fact]
    public async Task Nullable_arithmetic_projection_preserves_null()
    {
        await using var ctx = Make();
        // Project A + B (nullable) and (A ?? -1) * C; compare full nullable sequences.
        var got = (await ctx.Query<Row>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, Sum = r.A + r.B, Scaled = (r.A ?? -1) * r.C }).ToListAsync());
        var oracle = Oracle().OrderBy(r => r.Id)
            .Select(r => new { r.Id, Sum = r.A + r.B, Scaled = (r.A ?? -1) * r.C }).ToList();
        Assert.Equal(oracle.Count, got.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].Sum, got.First(x => x.Id == oracle[i].Id).Sum);
            Assert.Equal(oracle[i].Scaled, got.First(x => x.Id == oracle[i].Id).Scaled);
        }
    }

    [Fact]
    public async Task Nullable_equality_and_null_check()
    {
        await using var ctx = Make();
        // A == B (both must be non-null and equal in C#; SQL NULL=NULL is UNKNOWN, so row 4 excluded).
        var eqGot = (await ctx.Query<Row>().Where(r => r.A == r.B)
            .Select(r => new { r.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var eqOracle = Oracle().Where(r => r.A == r.B).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(eqOracle, eqGot);

        // A != B: C# true when exactly one is null OR both non-null and differ; SQL must include the
        // one-null rows (a common silent bug is dropping them).
        var neGot = (await ctx.Query<Row>().Where(r => r.A != r.B)
            .Select(r => new { r.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        var neOracle = Oracle().Where(r => r.A != r.B).Select(r => r.Id).OrderBy(x => x).ToArray();
        Assert.Equal(neOracle, neGot);
    }
}
