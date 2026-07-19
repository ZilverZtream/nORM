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
/// Ordered / top-N collection projections — <c>Select(b => new { Recent = b.Posts.OrderByDescending(p =>
/// p.Rank).Take(3).ToList() })</c> — emit a ROW_NUMBER partition window in the split-query child fetch. The
/// crux is that the element/tenant/global filters sit INSIDE the window (so the top-N is taken over the
/// visible, ordered set), not outside it (which would silently return the wrong rows).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderedCollectionProjectionTests
{
    [Table("OcpBlog")]
    private class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("OcpPost")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
        public int Rank { get; set; }
        public bool Active { get; set; }
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Blog>().HasKey(b => b.Id);
            mb.Entity<Post>().HasKey(p => p.Id);
        }
    }, ownsConnection: false);

    private static SqliteConnection Seed(string posts)
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE OcpBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE OcpPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL, Rank INTEGER NOT NULL, Active INTEGER NOT NULL);
            INSERT INTO OcpBlog VALUES (1, 'a'), (2, 'b');
            {posts}
            """;
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Top_n_per_parent_ordered_descending()
    {
        // Blog 1: ranks 1..5; Blog 2: ranks 10,20.
        using var cn = Seed("""
            INSERT INTO OcpPost VALUES (1,1,'p1',1,1),(2,1,'p2',2,1),(3,1,'p3',3,1),(4,1,'p4',4,1),(5,1,'p5',5,1),
                                       (6,2,'q1',10,1),(7,2,'q2',20,1);
            """);
        await using var ctx = Ctx(cn);
        var rows = (await NormAsyncExtensions.ToListAsync(ctx.Query<Blog>()
            .Select(b => new { b.Id, Top = b.Posts.OrderByDescending(p => p.Rank).Take(3).ToList() })))
            .OrderBy(r => r.Id).ToList();

        Assert.Equal(new[] { 5, 4, 3 }, rows.Single(r => r.Id == 1).Top.Select(p => p.Rank));   // top-3 desc
        Assert.Equal(new[] { 20, 10 }, rows.Single(r => r.Id == 2).Top.Select(p => p.Rank));    // fewer than N → all
    }

    [Fact]
    public async Task Top_n_is_taken_over_the_filtered_set_not_the_whole_collection()
    {
        // The pinning test. Blog 1: an INACTIVE post has the highest rank. Top-2 ACTIVE by rank must skip it.
        // If the Active filter were applied AFTER the row cap (outside the window), the window would pick the
        // inactive rank-100 row into the top-2 and then drop it, yielding only ONE row.
        using var cn = Seed("""
            INSERT INTO OcpPost VALUES (1,1,'active-lo',1,1),(2,1,'INACTIVE-hi',100,0),(3,1,'active-mid',50,1),(4,1,'active-30',30,1);
            """);
        await using var ctx = Ctx(cn);
        var top = (await NormAsyncExtensions.ToListAsync(ctx.Query<Blog>()
            .Where(b => b.Id == 1)
            .Select(b => new { b.Id, Top = b.Posts.Where(p => p.Active).OrderByDescending(p => p.Rank).Take(2).ToList() })))
            .Single().Top;

        Assert.Equal(new[] { 50, 30 }, top.Select(p => p.Rank));   // active-mid, active-30 — NOT the rank-100 inactive
    }

    [Fact]
    public async Task Order_by_then_by_breaks_ties_deterministically()
    {
        // Two posts share Rank=5; ThenBy Title ascending decides order.
        using var cn = Seed("""
            INSERT INTO OcpPost VALUES (1,1,'b',5,1),(2,1,'a',5,1),(3,1,'c',1,1);
            """);
        await using var ctx = Ctx(cn);
        var top = (await NormAsyncExtensions.ToListAsync(ctx.Query<Blog>()
            .Where(b => b.Id == 1)
            .Select(b => new { b.Id, Top = b.Posts.OrderByDescending(p => p.Rank).ThenBy(p => p.Title).Take(2).ToList() })))
            .Single().Top;

        Assert.Equal(new[] { "a", "b" }, top.Select(p => p.Title));   // both rank 5, ThenBy Title asc
    }

    [Fact]
    public async Task Skip_then_take_pages_within_each_parent()
    {
        using var cn = Seed("""
            INSERT INTO OcpPost VALUES (1,1,'p1',1,1),(2,1,'p2',2,1),(3,1,'p3',3,1),(4,1,'p4',4,1),(5,1,'p5',5,1);
            """);
        await using var ctx = Ctx(cn);
        var top = (await NormAsyncExtensions.ToListAsync(ctx.Query<Blog>()
            .Where(b => b.Id == 1)
            .Select(b => new { b.Id, Page = b.Posts.OrderByDescending(p => p.Rank).Skip(1).Take(2).ToList() })))
            .Single().Page;

        Assert.Equal(new[] { 4, 3 }, top.Select(p => p.Rank));   // ranks 5,4,3,2,1 → skip 1 (5) → take 2 → 4,3
    }

    [Fact]
    public async Task Ordered_top_n_with_element_projection()
    {
        using var cn = Seed("""
            INSERT INTO OcpPost VALUES (1,1,'p1',1,1),(2,1,'p2',2,1),(3,1,'p3',3,1);
            """);
        await using var ctx = Ctx(cn);
        var titles = (await NormAsyncExtensions.ToListAsync(ctx.Query<Blog>()
            .Where(b => b.Id == 1)
            .Select(b => new { b.Id, Titles = b.Posts.OrderByDescending(p => p.Rank).Take(2).Select(p => p.Title).ToList() })))
            .Single().Titles;

        Assert.Equal(new[] { "p3", "p2" }, titles);
    }

    [Fact]
    public async Task Skip_take_without_order_by_fails_loud()
    {
        using var cn = Seed("INSERT INTO OcpPost VALUES (1,1,'p1',1,1),(2,1,'p2',2,1);");
        await using var ctx = Ctx(cn);
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            NormAsyncExtensions.ToListAsync(ctx.Query<Blog>()
                .Select(b => new { b.Id, Top = b.Posts.Take(2).ToList() })));
    }
}
