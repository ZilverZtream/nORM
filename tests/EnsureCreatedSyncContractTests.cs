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
/// Pins the synchronous Database.EnsureCreated()/EnsureDeleted() (EF-parity): they create/drop the mapped
/// tables from the fluent model, order creation so a principal precedes its dependents (and drop in
/// reverse), are idempotent (a no-op call returns false), and produce usable tables. Truly synchronous —
/// no sync-over-async.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EnsureCreatedSyncContractTests
{
    [Table("SecBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("SecPost")]
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
            mb.Entity<Blog>().HasMany(b => b.Posts).WithOne(p => p.Blog!).HasForeignKey(p => p.BlogId, b => b.Id);
        }
    };

    private static bool TableExists(SqliteConnection cn, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name=$n";
        cmd.Parameters.AddWithValue("$n", name);
        return cmd.ExecuteScalar() != null;
    }

    [Fact]
    public void EnsureCreated_creates_the_mapped_tables_and_is_idempotent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        Assert.True(ctx.Database.EnsureCreated());   // first call creates
        Assert.True(TableExists(cn, "SecBlog"));
        Assert.True(TableExists(cn, "SecPost"));
        Assert.False(ctx.Database.EnsureCreated());  // second call: everything exists → no-op
    }

    [Fact]
    public void EnsureCreated_creates_only_the_missing_tables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SecBlog (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        Assert.True(ctx.Database.EnsureCreated());   // only SecPost was missing
        Assert.True(TableExists(cn, "SecPost"));
    }

    [Fact]
    public void EnsureDeleted_drops_the_mapped_tables_and_is_idempotent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        Assert.True(ctx.Database.EnsureCreated());
        Assert.True(ctx.Database.EnsureDeleted());    // drops both
        Assert.False(TableExists(cn, "SecBlog"));
        Assert.False(TableExists(cn, "SecPost"));
        Assert.False(ctx.Database.EnsureDeleted());   // nothing left to drop → no-op
    }

    [Fact]
    public async Task Sync_created_schema_supports_writes_and_reads()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = new DbContext(cn, new SqliteProvider(), Options(), ownsConnection: false);

        Assert.True(ctx.Database.EnsureCreated());

        ctx.Add(new Blog { Id = 1, Title = "b1" });
        ctx.Add(new Post { Id = 1, BlogId = 1, Text = "p1" });
        await ctx.SaveChangesAsync();

        Assert.Equal("b1", (await ctx.Query<Blog>().AsNoTracking().ToListAsync()).Single().Title);
        Assert.Equal(1, (await ctx.Query<Post>().AsNoTracking().ToListAsync()).Single().BlogId);
    }
}
