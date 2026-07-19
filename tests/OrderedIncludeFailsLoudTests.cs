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
/// Ordering or limiting an included collection (<c>Include(b =&gt; b.Posts.OrderByDescending(...))</c>) is not
/// yet supported. It must fail with a clear <see cref="NormUnsupportedFeatureException"/> naming the limitation
/// and the workaround — previously it threw a raw <see cref="System.InvalidCastException"/> from an internal
/// cast, which is an opaque crash rather than an actionable error.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderedIncludeFailsLoudTests
{
    [Table("OifBlog")]
    private class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("OifPost")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = "";
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Blog>().HasKey(b => b.Id);
            mb.Entity<Post>().HasKey(p => p.Id);
        }
    }, ownsConnection: false);

    [Fact]
    public void Ordered_include_throws_a_clear_unsupported_feature_error()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OifBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE OifPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Title TEXT NOT NULL);
                INSERT INTO OifBlog VALUES (1, 'a');
                INSERT INTO OifPost VALUES (1, 1, 'p1'), (2, 1, 'p2');
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = Ctx(cn);

        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ((INormQueryable<Blog>)ctx.Query<Blog>())
                .Include(b => b.Posts.OrderByDescending(p => p.Id).ToList())
                .ToList());
        Assert.Contains("Ordering or limiting an included collection", ex.Message);
    }
}
