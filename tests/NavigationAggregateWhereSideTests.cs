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
/// Predicate-context aggregates over owned (OwnsMany) and many-to-many collections —
/// <c>Where(p =&gt; p.Tags.Any())</c>, <c>Where(o =&gt; o.Lines.Count() &gt; 1)</c> — lower to correlated
/// subqueries. These collections are stored separately from ordinary relations, so the relation-based
/// predicate rewrite couldn't reach them (they failed loud with an unmapped-member error). The unfiltered
/// Any/Count shapes now translate; predicates, All, and filtered element types fail loud, never wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateWhereSideTests
{
    [Table("WaPost")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("WaTag")]
    private class Tag { [Key] public int Id { get; set; } public string Label { get; set; } = ""; public int Weight { get; set; } public bool Hidden { get; set; } }

    [Table("WaOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    private class Line { public int Id { get; set; } public int Amount { get; set; } }

    private static void CreateSchema(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WaPost (Id INTEGER PRIMARY KEY);
            CREATE TABLE WaTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Weight INTEGER NOT NULL, Hidden INTEGER NOT NULL);
            CREATE TABLE WaPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
            CREATE TABLE WaOrder (Id INTEGER PRIMARY KEY);
            CREATE TABLE WaLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO WaPost VALUES (1),(2),(3);
            INSERT INTO WaTag VALUES (1,'x',5,0),(2,'y',20,0);
            INSERT INTO WaPostTag VALUES (1,1),(1,2),(2,1);
            INSERT INTO WaOrder VALUES (1),(2),(3);
            INSERT INTO WaLine VALUES (1,1,10),(2,1,20),(3,2,5);
            """;
        cmd.ExecuteNonQuery();
    }

    private static void Configure(ModelBuilder mb)
    {
        mb.Entity<Post>().HasKey(p => p.Id);
        mb.Entity<Tag>().HasKey(t => t.Id);
        mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("WaPostTag", "PostId", "TagId");
        mb.Entity<Order>().HasKey(o => o.Id);
        mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "WaLine", foreignKey: "OrderId");
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        CreateSchema(cn);
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions { OnModelCreating = Configure });
    }

    [Fact]
    public void Where_m2m_Any_selects_owners_with_related_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ids = ctx.Query<Post>().Where(p => p.Tags.Any()).ToList().Select(p => p.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public void Where_m2m_Count_comparison()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ids = ctx.Query<Post>().Where(p => p.Tags.Count() >= 2).ToList().Select(p => p.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, ids);   // only post 1 has 2 tags
    }

    [Fact]
    public void Where_owned_Count_comparison()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ids = ctx.Query<Order>().Where(o => o.Lines.Count() > 1).ToList().Select(o => o.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, ids);   // only order 1 has >1 line
    }

    [Fact]
    public void Where_owned_Any()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ids = ctx.Query<Order>().Where(o => o.Lines.Any()).ToList().Select(o => o.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public void Where_m2m_Any_with_predicate_filters_related_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Posts with a tag labelled "y": only post 1 (tags x, y); post 2 has only "x".
        var ids = ctx.Query<Post>().Where(p => p.Tags.Any(t => t.Label == "y"))
            .ToList().Select(p => p.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Where_owned_Count_with_predicate()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Orders with more than one line whose Amount >= 10: order 1 has lines 10 and 20 → 2 (>1); order 2 has 5.
        var ids = ctx.Query<Order>().Where(o => o.Lines.Count(l => l.Amount >= 10) > 1)
            .ToList().Select(o => o.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Where_owned_Sum_comparison()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Order 1 line amounts 10 + 20 = 30 (> 25); order 2 = 5.
        var ids = ctx.Query<Order>().Where(o => o.Lines.Sum(l => l.Amount) > 25)
            .ToList().Select(o => o.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Where_m2m_Sum_comparison()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Post 1 tag weights 5 + 20 = 25 (>= 25); post 2 has tag weight 5.
        var ids = ctx.Query<Post>().Where(p => p.Tags.Sum(t => t.Weight) >= 25)
            .ToList().Select(p => p.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, ids);
    }

    [Fact]
    public void Where_m2m_Any_excludes_globally_filtered_related_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WaPost (Id INTEGER PRIMARY KEY);
                CREATE TABLE WaTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Weight INTEGER NOT NULL, Hidden INTEGER NOT NULL);
                CREATE TABLE WaPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                CREATE TABLE WaOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE WaLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO WaPost VALUES (10),(11);
                INSERT INTO WaTag VALUES (100,'ok',1,0),(101,'secret',1,1);
                -- Post 10 → a visible tag; post 11 → only a hidden tag.
                INSERT INTO WaPostTag VALUES (10,100),(11,101);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Tag>(t => !t.Hidden);
        opts.OnModelCreating = Configure;
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // The related global filter is applied inside the subquery: post 11's only tag is hidden, so it
        // has no visible tags and is excluded — not a plain bridge-row existence check.
        var ids = ctx.Query<Post>().Where(p => p.Tags.Any())
            .ToList().Select(p => p.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 10 }, ids);
    }
}
