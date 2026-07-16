using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins the fast-path x eager-load interaction: query shapes the fast-path executor
/// serves (property-equality Where, plain Take) must keep full Include behaviour when
/// an Include is present — children load on every parent, AsSplitQuery composes, and
/// included entities stay change-tracked so later edits persist. Guards the neighbour
/// class of the fast-path tracking kill: an Include silently dropped by a fast-path
/// route would return correct parents with EMPTY child collections.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FastPathIncludeInteractionContractTests
{
    [Table("FpiParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("FpiChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = "";
        public Parent? Parent { get; set; }
    }

    private static DbContext NewCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE FpiParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE FpiChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);" +
                "INSERT INTO FpiParent VALUES (1,'a'),(2,'b'),(3,'a');" +
                "INSERT INTO FpiChild VALUES (10,1,'c1'),(11,1,'c2'),(12,2,'c3'),(13,3,'c4');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent!).HasForeignKey(c => c.ParentId);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Include_with_fast_path_shaped_where_loads_children()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // Where(prop == literal) is exactly the fast-path list shape.
        var parents = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children)
            .Where(p => p.Name == "a")
            .OrderBy(p => p.Id)
            .ToList();

        Assert.Equal(new[] { 1, 3 }, parents.Select(p => p.Id).ToArray());
        Assert.Equal(new[] { "c1", "c2" }, parents[0].Children.OrderBy(c => c.Id).Select(c => c.Tag).ToArray());
        Assert.Equal(new[] { "c4" }, parents[1].Children.Select(c => c.Tag).ToArray());
    }

    [Fact]
    public void Include_with_take_loads_children()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        // Plain Take(n) is a fast-path shape too.
        var parents = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children)
            .OrderBy(p => p.Id)
            .Take(2)
            .ToList();

        Assert.Equal(new[] { 1, 2 }, parents.Select(p => p.Id).ToArray());
        Assert.Equal(2, parents[0].Children.Count);
        Assert.Single(parents[1].Children);
    }

    [Fact]
    public void Include_with_split_query_and_fast_path_shape_loads_children()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var parents = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .Where(p => p.Name == "a")
            .OrderBy(p => p.Id)
            .ToList();

        Assert.Equal(new[] { 1, 3 }, parents.Select(p => p.Id).ToArray());
        Assert.Equal(2, parents[0].Children.Count);
        Assert.Single(parents[1].Children);
    }

    [Fact]
    public async System.Threading.Tasks.Task Included_children_are_tracked_and_persist_edits()
    {
        using var ctx = NewCtx(out var cn);
        using var _cn = cn;

        var parent = ((INormQueryable<Parent>)ctx.Query<Parent>())
            .Include(p => p.Children)
            .Where(p => p.Name == "b")
            .ToList()
            .Single();
        parent.Children.Single().Tag = "edited";
        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Tag FROM FpiChild WHERE Id = 12";
        Assert.Equal("edited", (string)check.ExecuteScalar()!);
    }
}
