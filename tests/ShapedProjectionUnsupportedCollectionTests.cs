using System.Collections.Generic;
using System.Threading.Tasks;
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
/// Shaping an owned (OwnsMany) or many-to-many collection into a projection —
/// <c>Select(o =&gt; new { o.Id, Items = o.Items.ToList() })</c> — is not yet supported by the split-query
/// stitch, which is keyed on ordinary relations. Such a binding used to be admitted upstream and then
/// silently skipped when no relation was found, leaving the materializer expecting a stitched column that
/// never arrived and crashing with an opaque <c>ArgumentOutOfRangeException</c>. It now fails loud with an
/// actionable message pointing at Include or a separate query.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedProjectionUnsupportedCollectionTests
{
    [Table("SpuPost")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("SpuTag")]
    private class Tag { [Key] public int Id { get; set; } public string Label { get; set; } = ""; }

    [Table("SpuOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    private class Line { public int Id { get; set; } public int Amount { get; set; } }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SpuPost (Id INTEGER PRIMARY KEY);
                CREATE TABLE SpuTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                CREATE TABLE SpuPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                CREATE TABLE SpuOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE SpuLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO SpuPost VALUES (1);
                INSERT INTO SpuTag VALUES (1,'x');
                INSERT INTO SpuPostTag VALUES (1,1);
                INSERT INTO SpuOrder VALUES (1);
                INSERT INTO SpuLine VALUES (1,1,10);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("SpuPostTag", "PostId", "TagId");
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "SpuLine", foreignKey: "OrderId");
            }
        });
    }

    [Fact]
    public void Shaping_a_bare_many_to_many_collection_loads_the_related_entities()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var rows = ctx.Query<Post>().Select(p => new { p.Id, Tags = p.Tags.ToList() }).ToList();
        var post = Assert.Single(rows);
        Assert.Equal(1, post.Id);
        Assert.Equal("x", Assert.Single(post.Tags).Label);
    }

    [Fact]
    public async Task Shaping_a_bare_many_to_many_collection_loads_via_the_sync_execution_path_for_sqlite()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // SQLite routes ToListAsync through the synchronous materialize path, so the m2m projection loads.
        var rows = await ((INormQueryable<Post>)ctx.Query<Post>())
            .Select(p => new { p.Id, Tags = p.Tags.ToList() }).ToListAsync();
        Assert.Equal("x", Assert.Single(Assert.Single(rows).Tags).Label);
    }

    [Fact]
    public void Filtered_many_to_many_shaped_projection_still_fails_loud()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Post>().Select(p => new { p.Id, Tags = p.Tags.Where(t => t.Id > 0).ToList() }).ToList());
        Assert.Contains("many-to-many", ex.Message);
    }

    [Fact]
    public void Shaping_an_owned_collection_fails_loud_not_crash()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().Select(o => new { o.Id, Lines = o.Lines.ToList() }).ToList());
        Assert.Contains("owned", ex.Message);
    }
}
