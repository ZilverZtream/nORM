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
/// A navigation-aggregate filter in a PROJECTION must follow SQL three-valued logic exactly as the WHERE
/// side does. p.Children.All(c => c.Score > 5) is NOT EXISTS(a child where NOT the predicate); a NULL child
/// fails the predicate in C# ((int?)null > 5 is false), so All must be false — but the projection path
/// emitted a bare NOT over the child predicate, which stays UNKNOWN for a NULL child and wrongly kept All
/// true. Likewise p.Children.Where(c => c.Score != 5).Count() must count a NULL child (C# null != 5 is
/// true) but the bare `<>` dropped it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionNavAggregateNullSemanticsTests
{
    [Table("PnaParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("PnaChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int? Score { get; set; }
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PnaParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE PnaChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Score INTEGER NULL);
                INSERT INTO PnaParent (Id) VALUES (1), (2);
                INSERT INTO PnaChild (Id, ParentId, Score) VALUES
                    (1, 1, 10), (2, 1, NULL),
                    (3, 2, 10), (4, 2, 20);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Projection_All_over_nullable_child_predicate_matches_dotnet()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Parent>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, AllBig = p.Children.All(c => c.Score > 5) })
            .ToListAsync())
            .ToDictionary(r => r.Id, r => r.AllBig);

        // P1 children {10, null}: (int?)null > 5 is false, so the null child fails All -> false.
        Assert.False(rows[1]);   // BUG: true — bare NOT stayed UNKNOWN for the null child
        // P2 children {10, 20}: all > 5 -> true.
        Assert.True(rows[2]);
    }

    [Fact]
    public async Task Projection_filtered_count_with_not_equal_counts_nullable_child()
    {
        await using var ctx = CreateDb();

        var rows = (await ctx.Query<Parent>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, N = p.Children.Where(c => c.Score != 5).Count() })
            .ToListAsync())
            .ToDictionary(r => r.Id, r => r.N);

        // P1 {10, null}: 10 != 5 (true) and null != 5 (true in C#) -> count 2.
        Assert.Equal(2, rows[1]);   // BUG: 1 — the null child's `<>` was UNKNOWN, not counted
        Assert.Equal(2, rows[2]);
    }

    [Fact]
    public async Task Projection_not_equal_between_two_nullable_operands_is_false_when_both_null()
    {
        await using var ctx = CreateDb();

        // c.Score != c.Score: C# `null != null` is FALSE (and n != n is false for any n), so NOTHING counts.
        // Guards the both-null trap: a naive `<> OR IS NULL` rescue would wrongly count the null child.
        var rows = (await ctx.Query<Parent>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, N = p.Children.Where(c => c.Score != c.Score).Count() })
            .ToListAsync())
            .ToDictionary(r => r.Id, r => r.N);

        Assert.Equal(0, rows[1]);
        Assert.Equal(0, rows[2]);
    }
}
