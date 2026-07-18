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
/// A projected aggregate over a MANY-TO-MANY collection must lower to a correlated subquery that joins the
/// bridge table to the related table — one value per owner row, with the related entity's global/tenant
/// filters restricting visibility exactly as the loaded collection would. M2M links live in a separate
/// mapping structure from ordinary relations, so these aggregates previously fell through to the
/// outer-aggregate handler and silently collapsed the whole result to a single wrong row (e.g. `[3]`).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateManyToManyTests
{
    [Table("MaPost")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("MaTag")]
    private class Tag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
        public int Weight { get; set; }
        public bool Hidden { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MaPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE MaTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Weight INTEGER NOT NULL, Hidden INTEGER NOT NULL);
                CREATE TABLE MaPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO MaPost VALUES (1,'a'),(2,'b'),(3,'c');
                INSERT INTO MaTag VALUES (1,'x',10,0),(2,'y',20,0),(3,'secret',5,1);
                -- Post 1: tags 1,2,3 (tag 3 is Hidden); Post 2: tag 1; Post 3: none.
                INSERT INTO MaPostTag VALUES (1,1),(1,2),(1,3),(2,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Tag>(t => !t.Hidden);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Post>().HasKey(p => p.Id);
            mb.Entity<Tag>().HasKey(t => t.Id);
            mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("MaPostTag", "PostId", "TagId");
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Count_excludes_globally_filtered_related_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Post>().Select(p => new { p.Id, N = p.Tags.Count() })
            .ToList().OrderBy(r => r.Id).ToList();
        // Post 1 links 3 tags but tag 3 is Hidden → 2 visible; post 2 → 1; post 3 → 0.
        Assert.Equal(new[] { 2, 1, 0 }, rows.Select(r => r.N).ToArray());
    }

    [Fact]
    public void Any_reflects_visible_related_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Post>().Select(p => new { p.Id, Has = p.Tags.Any() })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { true, true, false }, rows.Select(r => r.Has).ToArray());
    }

    [Fact]
    public void Sum_over_a_related_column_excludes_filtered_rows()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Post>().Select(p => new { p.Id, W = p.Tags.Sum(t => t.Weight) })
            .ToList().OrderBy(r => r.Id).ToList();
        // Post 1: 10 + 20 (hidden tag 3's 5 excluded) = 30; post 2: 10; post 3: 0.
        Assert.Equal(new[] { 30, 10, 0 }, rows.Select(r => r.W).ToArray());
    }

    [Fact]
    public void Filtered_count_restricts_and_still_excludes_hidden()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Post>().Select(p => new { p.Id, N = p.Tags.Count(t => t.Weight >= 20) })
            .ToList().OrderBy(r => r.Id).ToList();
        // Weight >= 20 AND visible: post 1 → only tag 2 (20) → 1; post 2 → tag 1 (10) → 0.
        Assert.Equal(new[] { 1, 0, 0 }, rows.Select(r => r.N).ToArray());
    }

    [Fact]
    public void All_over_m2m_fails_loud_not_silently_wrong()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Post>().Select(p => new { p.Id, B = p.Tags.All(t => t.Weight > 0) }).ToList());
    }
}
