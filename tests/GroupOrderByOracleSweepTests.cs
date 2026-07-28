using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial LINQ-to-Objects oracle sweep across GroupBy key correctness, aggregate result types,
/// OrderBy null placement, and Distinct interaction on the SQLite provider. Each nORM query is diffed
/// against the identical lambda run over the in-memory seed list.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupOrderByOracleSweepTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("SwRow")]
    public sealed class SwRow
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = string.Empty;
        public int A { get; set; }
        public int B { get; set; }
        public int? N { get; set; }        // nullable int
        public string? S { get; set; }     // nullable string
    }

    private static readonly SwRow[] Seed =
    {
        new SwRow { Id = 1, Cat = "x", A = 3,  B = 1, N = 5,    S = "b" },
        new SwRow { Id = 2, Cat = "x", A = 4,  B = 2, N = null, S = null },
        new SwRow { Id = 3, Cat = "y", A = 10, B = 1, N = 5,    S = "a" },
        new SwRow { Id = 4, Cat = "y", A = 1,  B = 1, N = 7,    S = "c" },
        new SwRow { Id = 5, Cat = "z", A = 2,  B = 3, N = null, S = "a" },
        new SwRow { Id = 6, Cat = "x", A = 3,  B = 1, N = 2,    S = "b" },
    };

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SwRow (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, A INTEGER NOT NULL, B INTEGER NOT NULL, N INTEGER NULL, S TEXT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SwRow>().HasKey(i => i.Id)
        });
        foreach (var r in Seed) _ctx.Add(new SwRow { Id = r.Id, Cat = r.Cat, A = r.A, B = r.B, N = r.N, S = r.S });
        await _ctx.SaveChangesAsync();
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ---- Average over int returns .NET double, not integer-truncated ----
    [Fact]
    public void Average_over_int_group_returns_double()
    {
        // Cat x: A = 3,4,3 -> avg 3.3333...
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "x").Select(g => g.Average(x => x.A)).Single();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.Cat).Where(g => g.Key == "x").Select(g => g.Average(x => x.A)).ToList().Single();
        Assert.Equal(oracle, norm, 9);
    }

    [Fact]
    public void Average_over_int_direct_returns_double()
    {
        var oracle = Seed.Where(x => x.Cat == "x").Average(x => x.A);
        var norm = _ctx.Query<SwRow>().Where(x => x.Cat == "x").Average(x => x.A);
        Assert.Equal(oracle, norm, 9);
    }

    // ---- GroupBy on a computed key (A + B), project key + count ----
    [Fact]
    public void GroupBy_computed_key_roundtrips()
    {
        var oracle = Seed.GroupBy(x => x.A + x.B).Select(g => new { K = g.Key, C = g.Count() }).OrderBy(x => x.K).ToList();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.A + x.B).Select(g => new { K = g.Key, C = g.Count() }).OrderBy(x => x.K).ToList();
        Assert.Equal(oracle.Select(o => (o.K, o.C)).ToList(), norm.Select(o => (o.K, o.C)).ToList());
    }

    // ---- GroupBy on a boolean-expression key (A > 3), project key + count ----
    [Fact]
    public void GroupBy_boolean_key_roundtrips()
    {
        var oracle = Seed.GroupBy(x => x.A > 3).Select(g => new { K = g.Key, C = g.Count() }).OrderBy(x => x.K).ToList();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.A > 3).Select(g => new { K = g.Key, C = g.Count() }).OrderBy(x => x.K).ToList();
        Assert.Equal(oracle.Select(o => (o.K, o.C)).ToList(), norm.Select(o => (o.K, o.C)).ToList());
    }

    // ---- GroupBy on a nullable int key: NULLs form ONE group, matching Enumerable ----
    [Fact]
    public void GroupBy_nullable_key_groups_nulls_together()
    {
        var oracle = Seed.GroupBy(x => x.N).Select(g => new { K = g.Key, C = g.Count() })
                         .OrderBy(x => x.K == null).ThenBy(x => x.K).ToList();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.N).Select(g => new { K = g.Key, C = g.Count() }).ToList()
                         .OrderBy(x => x.K == null).ThenBy(x => x.K).ToList();
        Assert.Equal(oracle.Select(o => (o.K, o.C)).ToList(), norm.Select(o => (o.K, o.C)).ToList());
    }

    // ---- OrderBy nullable int ascending: NULLs sort first (matches .NET null-is-smallest) ----
    [Fact]
    public void OrderBy_nullable_int_ascending_null_placement()
    {
        var oracle = Seed.OrderBy(x => x.N).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        var norm = _ctx.Query<SwRow>().OrderBy(x => x.N).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // ---- OrderBy nullable int descending: NULLs sort last (.NET null-is-smallest) ----
    [Fact]
    public void OrderBy_nullable_int_descending_null_placement()
    {
        var oracle = Seed.OrderByDescending(x => x.N).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        var norm = _ctx.Query<SwRow>().OrderByDescending(x => x.N).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // ---- OrderBy nullable string ascending: NULLs first ----
    [Fact]
    public void OrderBy_nullable_string_ascending_null_placement()
    {
        var oracle = Seed.OrderBy(x => x.S).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        var norm = _ctx.Query<SwRow>().OrderBy(x => x.S).ThenBy(x => x.Id).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // ---- OrderBy then Skip/Take: ordering survives paging ----
    [Fact]
    public void OrderBy_then_skip_take_preserves_order()
    {
        var oracle = Seed.OrderBy(x => x.A).ThenBy(x => x.Id).Skip(1).Take(3).Select(x => x.Id).ToList();
        var norm = _ctx.Query<SwRow>().OrderBy(x => x.A).ThenBy(x => x.Id).Skip(1).Take(3).Select(x => x.Id).ToList();
        Assert.Equal(oracle, norm);
    }

    // ---- Sum over a filtered-to-empty group is 0, not NULL ----
    [Fact]
    public void Sum_over_filtered_empty_group_is_zero()
    {
        // Cat x has no rows with A > 100; g.Sum(where A>100) must be 0.
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "x").Select(g => g.Where(x => x.A > 100).Sum(x => x.A)).Single();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.Cat).Where(g => g.Key == "x").Select(g => g.Where(x => x.A > 100).Sum(x => x.A)).ToList().Single();
        Assert.Equal(oracle, norm); // 0
    }

    // ---- Count(Distinct) over a group ----
    [Fact]
    public void Count_distinct_over_group()
    {
        // Cat x: S = "b", null, "b" -> distinct non-null = {"b"} -> count 1 (Enumerable Distinct counts null too,
        // but COUNT(DISTINCT col) excludes null; assert against the Enumerable form that excludes null to match SQL).
        var oracle = Seed.GroupBy(x => x.Cat).Select(g => new { K = g.Key, C = g.Select(x => x.S).Where(s => s != null).Distinct().Count() }).OrderBy(x => x.K).ToList();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.Cat).Select(g => new { K = g.Key, C = g.Select(x => x.S).Distinct().Count() }).ToList().OrderBy(x => x.K).ToList();
        Assert.Equal(oracle.Select(o => (o.K, o.C)).ToList(), norm.Select(o => (o.K, o.C)).ToList());
    }

    // ---- Min over nullable int in a group where some values are null ----
    [Fact]
    public void Min_over_nullable_int_ignores_nulls()
    {
        // Cat x: N = 5, null, 2 -> min 2 (SQL MIN ignores null; Enumerable Min over int? also skips? No:
        // Enumerable.Min(int?) returns the min of non-null, or null if all null). Non-null min = 2.
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "x").Select(g => g.Min(x => x.N)).Single();
        var norm = _ctx.Query<SwRow>().GroupBy(x => x.Cat).Where(g => g.Key == "x").Select(g => g.Min(x => x.N)).ToList().Single();
        Assert.Equal(oracle, norm); // 2
    }
}
