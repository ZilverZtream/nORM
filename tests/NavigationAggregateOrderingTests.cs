using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Ordering by an owned/many-to-many collection aggregate — <c>OrderByDescending(o =&gt; o.Lines.Count())</c> —
/// composes: the ORDER BY key renders through the same correlated-subquery emit as a projected aggregate, so
/// these non-relation collections sort correctly rather than crashing on the relation-keyed path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateOrderingTests
{
    [Table("ObPost")]
    private class Post { [Key] public int Id { get; set; } public List<Tag> Tags { get; set; } = new(); }
    [Table("ObTag")]
    private class Tag { [Key] public int Id { get; set; } }
    [Table("ObOrder")]
    private class Order { [Key] public int Id { get; set; } public List<Line> Lines { get; set; } = new(); }
    private class Line { public int Id { get; set; } public int Amount { get; set; } }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ObPost (Id INTEGER PRIMARY KEY);
                CREATE TABLE ObTag (Id INTEGER PRIMARY KEY);
                CREATE TABLE ObPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                CREATE TABLE ObOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE ObLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO ObPost VALUES (1),(2),(3);
                INSERT INTO ObTag VALUES (1),(2);
                INSERT INTO ObPostTag VALUES (1,1),(1,2),(2,1);
                INSERT INTO ObOrder VALUES (1),(2),(3);
                INSERT INTO ObLine VALUES (1,1,10),(2,1,20),(3,2,5);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("ObPostTag", "PostId", "TagId");
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "ObLine", foreignKey: "OrderId");
            }
        });
    }

    [Fact]
    public void orderby_owned_count()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        // Line counts: order 1 -> 2, order 2 -> 1, order 3 -> 0. Descending by count: 1,2,3.
        var ids = ctx.Query<Order>().OrderByDescending(o => o.Lines.Count()).ToList().Select(o => o.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public void orderby_m2m_count()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        // Tag counts: post 1 -> 2, post 2 -> 1, post 3 -> 0. Descending: 1,2,3.
        var ids = ctx.Query<Post>().OrderByDescending(p => p.Tags.Count()).ToList().Select(p => p.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 3 }, ids);
    }
}
