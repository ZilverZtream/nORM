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
/// GroupBy over a JOIN result: the group key references a column from the outer table
/// (`Join(...).GroupBy(x => x.OuterMember)`). Previously the group-key visitor forced a single
/// (mapping, alias) that overrode the join's per-side alias mapping, so an OUTER column was
/// qualified with the INNER alias — invalid SQL (`no such column: T1.PVal`). Now the key resolves
/// through _correlatedParams. Verified against LINQ-to-objects. (Aggregates over a JOINED member
/// are a separate, still-open path — not covered here.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class JoinGroupByKeyTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("JgkParent")]
    private sealed class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int PVal { get; set; }
        public string Region { get; set; } = "";
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("JgkChild")]
    private sealed class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int ChildVal { get; set; }
    }

    private static readonly Parent[] Parents =
    {
        new() { Id = 1, PVal = 10, Region = "N" }, new() { Id = 2, PVal = 10, Region = "S" },
        new() { Id = 3, PVal = 20, Region = "N" }, new() { Id = 4, PVal = 30, Region = "S" },
    };
    private static readonly Child[] Children =
    {
        new() { Id = 1, ParentId = 1, ChildVal = 5 }, new() { Id = 2, ParentId = 1, ChildVal = 7 },
        new() { Id = 3, ParentId = 2, ChildVal = 3 }, new() { Id = 4, ParentId = 3, ChildVal = 9 },
        new() { Id = 5, ParentId = 3, ChildVal = 1 }, // parent 4 has no children (inner join drops it)
    };

    private static DbContext Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE JgkParent (Id INTEGER PRIMARY KEY, PVal INTEGER NOT NULL, Region TEXT NOT NULL);" +
                              "CREATE TABLE JgkChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, ChildVal INTEGER NOT NULL);";
            foreach (var p in Parents) cmd.CommandText += $"INSERT INTO JgkParent VALUES ({p.Id},{p.PVal},'{p.Region}');";
            foreach (var c in Children) cmd.CommandText += $"INSERT INTO JgkChild VALUES ({c.Id},{c.ParentId},{c.ChildVal});";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Join_groupby_outer_key_count_matches_linq()
    {
        var expected = Parents.Join(Children, p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.PVal).Select(g => new { g.Key, C = g.Count() }).OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Parent>().Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.PVal).Select(g => new { g.Key, C = g.Count() }).OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.C)), actual.Select(r => (r.Key, r.C)));
    }

    [Fact]
    public void Join_groupby_aggregates_over_joined_member_match_linq()
    {
        var expected = Parents.Join(Children, p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.PVal)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal), Mn = g.Min(x => x.ChildVal), Mx = g.Max(x => x.ChildVal) })
            .OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Parent>().Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.PVal)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal), Mn = g.Min(x => x.ChildVal), Mx = g.Max(x => x.ChildVal) })
            .OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.C, r.S, r.Mn, r.Mx)),
                     actual.Select(r => (r.Key, r.C, r.S, r.Mn, r.Mx)));
    }

    [Fact]
    public void Join_groupby_inner_key_and_filtered_aggregate_match_linq()
    {
        // Inner-table member as the GROUP key, plus a filtered aggregate Count(pred) over an
        // inner member and a Sum over the outer member — all previously declined.
        var expected = Parents.Join(Children, p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.ChildVal)
            .Select(g => new { g.Key, N = g.Count(x => x.PVal >= 10), OuterSum = g.Sum(x => x.PVal) })
            .OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Parent>().Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.ChildVal)
            .Select(g => new { g.Key, N = g.Count(x => x.PVal >= 10), OuterSum = g.Sum(x => x.PVal) })
            .OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.N, r.OuterSum)), actual.Select(r => (r.Key, r.N, r.OuterSum)));
    }

    [Fact]
    public void Join_groupby_having_count_with_inner_sum_matches_linq()
    {
        // HAVING on Count() (no member) + Sum over an inner member in the SELECT. (HAVING that
        // aggregates a JOINED member — Where(g => g.Sum(x => x.inner) > k) — is a separate open
        // gap: the join projection context is gone by HAVING-translation time.)
        var expected = Parents.Join(Children, p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.PVal).Where(g => g.Count() > 1)
            .Select(g => new { g.Key, S = g.Sum(x => x.ChildVal) }).OrderBy(r => r.Key).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Parent>().Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
            .GroupBy(x => x.PVal).Where(g => g.Count() > 1)
            .Select(g => new { g.Key, S = g.Sum(x => x.ChildVal) }).OrderBy(r => r.Key).ToList();
        Assert.Equal(expected.Select(r => (r.Key, r.S)), actual.Select(r => (r.Key, r.S)));
    }

    [Fact]
    public void Join_groupby_composite_outer_key_count_matches_linq()
    {
        var expected = Parents.Join(Children, p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, p.Region, c.ChildVal })
            .GroupBy(x => new { x.PVal, x.Region }).Select(g => new { g.Key.PVal, g.Key.Region, C = g.Count() })
            .OrderBy(r => r.PVal).ThenBy(r => r.Region).ToList();
        using var ctx = Ctx();
        var actual = ctx.Query<Parent>().Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, p.Region, c.ChildVal })
            .GroupBy(x => new { x.PVal, x.Region }).Select(g => new { g.Key.PVal, g.Key.Region, C = g.Count() })
            .OrderBy(r => r.PVal).ThenBy(r => r.Region).ToList();
        Assert.Equal(expected.Select(r => (r.PVal, r.Region, r.C)), actual.Select(r => (r.PVal, r.Region, r.C)));
    }
}
