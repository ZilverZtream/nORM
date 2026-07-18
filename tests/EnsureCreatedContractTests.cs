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
/// Pins DbContext.Database.EnsureCreatedAsync: it creates the mapped tables from the fluent
/// model, orders creation so a referenced (principal) table precedes its dependents, is
/// idempotent (a second call creates nothing and returns false), and produces usable tables.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EnsureCreatedContractTests
{
    [Table("EcBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public System.Collections.Generic.List<Post> Posts { get; set; } = new();
    }

    [Table("EcPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Text { get; set; } = "";
        public Blog? Blog { get; set; }
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Blog>().HasKey(b => b.Id);
            mb.Entity<Post>().HasKey(p => p.Id);
            // Post.BlogId references Blog.Id — Post must be created after Blog on strict providers.
            mb.Entity<Blog>().HasMany(b => b.Posts).WithOne(p => p.Blog!).HasForeignKey(p => p.BlogId, b => b.Id);
        }
    };

    [Fact]
    public async Task EnsureCreated_creates_the_mapped_tables_and_they_are_usable()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        var created = await ctx.Database.EnsureCreatedAsync();
        Assert.True(created);

        // The tables exist and round-trip through the normal write/read paths.
        ctx.Add(new Blog { Id = 1, Title = "b1" });
        ctx.Add(new Post { Id = 1, BlogId = 1, Text = "p1" });
        await ctx.SaveChangesAsync();

        Assert.Equal("b1", (await ctx.Query<Blog>().AsNoTracking().ToListAsync()).Single().Title);
        Assert.Equal(1, (await ctx.Query<Post>().AsNoTracking().ToListAsync()).Single().BlogId);
    }

    [Fact]
    public async Task EnsureCreated_is_idempotent()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        Assert.True(await ctx.Database.EnsureCreatedAsync());   // first call creates
        Assert.False(await ctx.Database.EnsureCreatedAsync());  // second call: everything exists
    }

    [Fact]
    public async Task EnsureCreated_creates_only_the_missing_tables()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        // Blog already exists (created out of band); EnsureCreated must add only Post.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EcBlog (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        await using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        Assert.True(await ctx.Database.EnsureCreatedAsync());   // Post was missing → created
        // Both usable now.
        ctx.Add(new Blog { Id = 5, Title = "b5" });
        ctx.Add(new Post { Id = 5, BlogId = 5, Text = "p5" });
        await ctx.SaveChangesAsync();
        Assert.Equal(5, (await ctx.Query<Post>().AsNoTracking().ToListAsync()).Single().Id);
    }
}
