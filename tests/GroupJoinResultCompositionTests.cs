using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Operators applied AFTER a group-join projection must operate on the grouped
/// result sequence, not on the flat joined row set the SQL produces — a server
/// OFFSET or LIMIT on the joined rows would truncate a parent's children
/// mid-group. Group-join results are assembled post-materialization, so these
/// compositions either evaluate client-side or must fail closed; silently
/// paging the joined rows is never acceptable.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GroupJoinResultCompositionTests
{
    private class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class Pet
    {
        public int Id { get; set; }
        public int OwnerId { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Person(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Pet(Id INTEGER, OwnerId INTEGER, Name TEXT);" +
                "INSERT INTO Person VALUES(1,'Alice');" +
                "INSERT INTO Person VALUES(2,'Bob');" +
                "INSERT INTO Person VALUES(3,'Cara');" +
                "INSERT INTO Pet VALUES(1,1,'Fido');" +
                "INSERT INTO Pet VALUES(2,1,'Rex');" +
                "INSERT INTO Pet VALUES(3,2,'Whiskers');" +
                "INSERT INTO Pet VALUES(4,3,'Polly');" +
                "INSERT INTO Pet VALUES(5,3,'Kiwi');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static IQueryable<(string Name, int PetCount)> GroupedCounts(DbContext ctx)
        => ctx.Query<Person>()
            .OrderBy(p => p.Id)
            .GroupJoin(ctx.Query<Pet>(), p => p.Id, pet => pet.OwnerId,
                (p, pets) => ValueTuple.Create(p.Name, pets.Count()));

    [Fact]
    public void Skip_after_group_join_projection_skips_whole_groups()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var results = GroupedCounts(ctx).Skip(1).ToList();

        // Alice (2 pets) is skipped as ONE group; Bob and Cara stay intact.
        Assert.Equal(2, results.Count);
        Assert.Equal(("Bob", 1), results[0]);
        Assert.Equal(("Cara", 2), results[1]);
    }

    [Fact]
    public void Take_after_group_join_projection_takes_whole_groups()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var results = GroupedCounts(ctx).Take(1).ToList();

        // Alice's group must arrive complete with both pets counted.
        var single = Assert.Single(results);
        Assert.Equal(("Alice", 2), single);
    }

    [Fact]
    public void First_after_group_join_projection_returns_a_complete_group()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var first = GroupedCounts(ctx).First();

        // A server LIMIT 1 on the joined rows would see only one of Alice's pets.
        Assert.Equal(("Alice", 2), first);
    }

    [Fact]
    public void Last_after_group_join_projection_returns_the_last_group()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Cara has TWO pets: the reversed-ORDER-BY LIMIT 1 shortcut on the joined
        // rows would see only one of them.
        var last = GroupedCounts(ctx).Last();

        Assert.Equal(("Cara", 2), last);
    }

    [Fact]
    public void Element_at_after_group_join_projection_indexes_whole_groups()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(("Bob", 1), GroupedCounts(ctx).ElementAt(1));
        Assert.Equal(("Cara", 2), GroupedCounts(ctx).ElementAt(2));
    }

    [Fact]
    public void Aggregates_after_group_join_projection_reduce_group_results()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Pet counts per group are 2, 1, 2 — a server-side aggregate over the flat
        // joined rows could never produce these values.
        Assert.Equal(3, GroupedCounts(ctx).Count());
        Assert.Equal(5, GroupedCounts(ctx).Sum(g => g.Item2));
        Assert.True(GroupedCounts(ctx).Any(g => g.Item2 == 2));
        Assert.False(GroupedCounts(ctx).All(g => g.Item2 == 2));
    }

    [Fact]
    public void Take_last_after_group_join_projection_keeps_the_final_groups_complete()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var results = GroupedCounts(ctx).TakeLast(2).ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal(("Bob", 1), results[0]);
        Assert.Equal(("Cara", 2), results[1]);
    }

    [Fact]
    public void Reverse_after_group_join_projection_reverses_whole_groups()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var results = GroupedCounts(ctx).Reverse().ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(("Cara", 2), results[0]);
        Assert.Equal(("Alice", 2), results[2]);
    }

    [Fact]
    public void Distinct_after_group_join_projection_dedups_result_rows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Project only the pet count: counts are 2, 1, 1 -> distinct = {2, 1}.
        var counts = ctx.Query<Person>()
            .OrderBy(p => p.Id)
            .GroupJoin(ctx.Query<Pet>(), p => p.Id, pet => pet.OwnerId,
                (p, pets) => pets.Count())
            .Distinct()
            .ToList();

        Assert.Equal(2, counts.Count);
        Assert.Contains(2, counts);
        Assert.Contains(1, counts);
    }
}
