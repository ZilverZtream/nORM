using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// Pins the AsOf boundary for shaping an owned (OwnsMany) or many-to-many collection into a projection.
/// An owned shaped projection reconstructs the historical era through the {Table}_History window (the owned
/// history carries the owner FK, built from the introspected physical column set) — the live rows must not
/// leak in, and a per-element filter applies to the reconstructed values. A many-to-many shaped projection
/// under AsOf fails loud for the same reason a many-to-many Include does: the association table is raw and
/// unversioned, so the membership at the timestamp is unknowable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedM2mShapedProjectionTemporalTests
{
    [Table("OmtsPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    // No [Table]: OwnsMany(tableName: ...) must reach the owned type's own mapping for the temporal bootstrap.
    public class Line
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    [Table("OmtsBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("OmtsTag")]
    public class Tag { [Key] public int Id { get; set; } public string Label { get; set; } = ""; }

    private static async Task<DateTime> ServerNow(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    private static DbContext BootOwned(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OmtsPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE OmtsLine (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Line>(p => p.Lines, tableName: "OmtsLine", foreignKey: "PostId")
        };
        opts.EnableTemporalVersioning();
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    /// <summary>Seeds one line ("v1"), snapshots t1, then edits it to "v2" and adds a second line after t1.</summary>
    private static async Task<DateTime> SeedTwoEras(DbContext ctx, SqliteConnection cn)
    {
        var post = new Post { Id = 1, Title = "p" };
        post.Lines.Add(new Line { Id = 1, Text = "v1" });
        ctx.Add(post);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNow(cn);
        await Task.Delay(60);
        var tracked = (await ctx.Query<Post>().Where(p => p.Id == 1).ToListAsync()).Single();
        tracked.Lines.Single().Text = "v2";                       // edit after t1
        tracked.Lines.Add(new Line { Id = 2, Text = "extra" });   // add after t1
        await ctx.SaveChangesAsync();
        return t1;
    }

    [Fact]
    public async Task Owned_shaped_projection_under_as_of_reconstructs_the_era()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; await using var ctx = BootOwned(cn);
        var t1 = await SeedTwoEras(ctx, cn);

        // At t1 the post had exactly one line, "v1". The live "v2"/"extra" must reconstruct away.
        var historic = await ((INormQueryable<Post>)ctx.Query<Post>())
            .Select(p => new { p.Id, Lines = p.Lines.ToList() }).AsOf(t1).ToListAsync();
        Assert.Equal(new[] { "v1" }, historic.Single().Lines.OrderBy(l => l.Id).Select(l => l.Text).ToArray());
    }

    [Fact]
    public async Task Filtered_owned_shaped_projection_under_as_of_filters_the_reconstructed_values()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; await using var ctx = BootOwned(cn);
        var t1 = await SeedTwoEras(ctx, cn);

        // The filter matches the HISTORICAL value ("v1"); if it ran against the live row ("v2") it would match
        // nothing, so a non-empty result proves the predicate applied to the reconstructed era.
        var want = "v1";
        var historic = await ((INormQueryable<Post>)ctx.Query<Post>())
            .Select(p => new { p.Id, Lines = p.Lines.Where(l => l.Text == want).ToList() }).AsOf(t1).ToListAsync();
        Assert.Equal("v1", historic.Single().Lines.Single().Text);
    }

    [Fact]
    public async Task Owned_shaped_projection_without_as_of_reads_the_live_era()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; await using var ctx = BootOwned(cn);
        await SeedTwoEras(ctx, cn);

        var live = ctx.Query<Post>().Select(p => new { p.Id, Lines = p.Lines.ToList() }).ToList();
        Assert.Equal(new[] { "extra", "v2" }, live.Single().Lines.Select(l => l.Text).OrderBy(t => t).ToArray());
    }

    [Fact]
    public async Task M2m_shaped_projection_under_as_of_fails_loud()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OmtsBlog (Id INTEGER PRIMARY KEY);
                CREATE TABLE OmtsTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                CREATE TABLE OmtsBlogTag (BlogId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Blog>().HasMany<Tag>(b => b.Tags).WithMany().UsingTable("OmtsBlogTag", "BlogId", "TagId");
            }
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        ctx.Add(new Tag { Id = 1, Label = "x" });
        ctx.Add(new Blog { Id = 1 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNow(cn);

        // The association table (OmtsBlogTag) is raw and unversioned, so the membership at t1 is unknowable —
        // a shaped m2m projection under AsOf must fail loud exactly as a m2m Include does.
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ((INormQueryable<Blog>)ctx.Query<Blog>()).Select(b => new { b.Id, Tags = b.Tags.ToList() }).AsOf(t1).ToListAsync());
        Assert.Contains("association table is not versioned", ex.Message, StringComparison.Ordinal);
    }
}
