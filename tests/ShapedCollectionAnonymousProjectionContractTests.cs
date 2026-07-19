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
/// Pins shaped/bare collection projections into an ANONYMOUS outer type:
/// <c>Select(o =&gt; new { o.Id, Lines = o.Lines.Where(p).Select(l =&gt; new {..}).ToList() })</c>. An anon
/// type is immutable, so the collection member can't be assigned after construction; the materializer
/// constructs an empty mutable <see cref="List{T}"/> for the member and the split-query stitch populates it
/// in place. Covers anon/scalar/DTO element projections, bare entity collections, a filter, empty matches,
/// two distinct collection members, the scalars-only regression, and the ToArray boundary.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionAnonymousProjectionContractTests
{
    [Table("AnsOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("AnsLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    [Table("AnsTag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Name { get; set; } = "";
    }

    public class LineDto { public string Sku { get; set; } = ""; public int Qty { get; set; } }

    private static DbContext Ctx(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AnsOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE AnsLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL, Qty INTEGER NOT NULL);
                CREATE TABLE AnsTag (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Name TEXT NOT NULL);
                INSERT INTO AnsOrder VALUES (1,'o1'),(2,'o2'),(3,'o3');
                INSERT INTO AnsLine VALUES (1,1,'a',3),(2,1,'b',8),(3,2,'c',5);
                INSERT INTO AnsTag VALUES (1,1,'red'),(2,1,'blue'),(3,2,'green');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Tag>().HasKey(t => t.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            mb.Entity<Order>().HasMany(o => o.Tags).WithOne().HasForeignKey(t => t.OrderId, o => o.Id);
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void anon_element_projection_shapes_and_stitches_per_parent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, Lines = o.Lines.Select(l => new { l.Sku, l.Qty }).ToList() })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(3, rows.Count);
        Assert.Equal(new[] { "a:3", "b:8" }, rows[0].Lines.OrderBy(l => l.Sku).Select(l => $"{l.Sku}:{l.Qty}").ToArray());
        Assert.Equal(new[] { "c:5" }, rows[1].Lines.Select(l => $"{l.Sku}:{l.Qty}").ToArray());
        Assert.Empty(rows[2].Lines);   // order 3 has no lines → empty
    }

    [Fact]
    public void scalar_element_projection_with_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, o.Code, Skus = o.Lines.Where(l => l.Qty > 4).Select(l => l.Sku).ToList() })
            .ToList().OrderBy(r => r.Code).ToList();
        Assert.Equal(new[] { "b" }, rows[0].Skus);   // o1 qty>4 → b(8)
        Assert.Equal(new[] { "c" }, rows[1].Skus);   // o2 qty>4 → c(5)
        Assert.Empty(rows[2].Skus);
    }

    [Fact]
    public void named_dto_element_inside_anon_outer()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, Lines = o.Lines.Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty }).ToList() })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { "a:3", "b:8" }, rows[0].Lines.OrderBy(l => l.Sku).Select(l => $"{l.Sku}:{l.Qty}").ToArray());
        Assert.Equal(new[] { "c:5" }, rows[1].Lines.Select(l => $"{l.Sku}:{l.Qty}").ToArray());
    }

    [Fact]
    public void bare_entity_collection_in_anon_outer()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var rows = ctx.Query<Order>()
            .Select(o => new { o.Id, o.Lines })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { 3, 8 }, rows[0].Lines.OrderBy(l => l.Qty).Select(l => l.Qty).ToArray());
        Assert.Single(rows[1].Lines);
        Assert.Empty(rows[2].Lines);
    }

    [Fact]
    public void two_distinct_collection_members_each_populate_independently()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var rows = ctx.Query<Order>()
            .Select(o => new
            {
                o.Id,
                Skus = o.Lines.Select(l => l.Sku).ToList(),
                Tags = o.Tags.Select(t => t.Name).ToList(),
            })
            .ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { "a", "b" }, rows[0].Skus.OrderBy(s => s).ToArray());
        Assert.Equal(new[] { "blue", "red" }, rows[0].Tags.OrderBy(s => s).ToArray());
        Assert.Equal(new[] { "c" }, rows[1].Skus);
        Assert.Equal(new[] { "green" }, rows[1].Tags);
        Assert.Empty(rows[2].Skus);
        Assert.Empty(rows[2].Tags);
    }

    // Regression: an anon projection WITHOUT a collection must be byte-identical to before.
    [Fact]
    public void scalars_only_anon_still_works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        var rows = ctx.Query<Order>().Select(o => new { o.Id, o.Code }).ToList().OrderBy(r => r.Id).ToList();
        Assert.Equal(new[] { "o1", "o2", "o3" }, rows.Select(r => r.Code).ToArray());
    }

    [Fact]
    public void two_projections_of_the_same_nav_fail_loud_not_silent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        // Both members project o.Lines with different filters. The per-nav detection maps collide, so one
        // would silently come back empty — fail loud instead of returning wrong data.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>()
                .Select(o => new
                {
                    o.Id,
                    Big = o.Lines.Where(l => l.Qty > 4).Select(l => l.Sku).ToList(),
                    Small = o.Lines.Where(l => l.Qty <= 4).Select(l => l.Sku).ToList(),
                })
                .ToList());
        Assert.Contains("Lines", ex.Message);
    }

    [Fact]
    public void toarray_collection_member_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Ctx(cn);
        // A T[] member is fixed-size and can't be populated in place after the immutable anon is constructed.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>()
                .Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToArray() })
                .ToList());
        Assert.Contains("ToList", ex.Message);
    }
}
