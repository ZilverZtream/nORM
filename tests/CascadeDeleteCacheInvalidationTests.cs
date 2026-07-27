using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// When deleting a parent cascades (at the database level) to child rows that were never loaded, those child
/// rows are gone from the database — so any Cacheable() result over the child table must be invalidated. The
/// SaveChanges cache-invalidation set was built only from the tracked/changed entities' tables, so the child
/// table was never invalidated and a later Cacheable() query replayed the cascade-deleted rows (stale read).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CascadeDeleteCacheInvalidationTests
{
    [Table("CdcBlog")]
    private sealed class Blog
    {
        [Key] public int Id { get; set; }
        public List<Post> Posts { get; set; } = new();
    }

    [Table("CdcPost")]
    private sealed class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
    }

    [Fact]
    public async Task Cascade_delete_of_unloaded_children_invalidates_child_table_cache()
    {
        using var cache = new NormMemoryCacheProvider();
        // Foreign Keys=True so SQLite actually enforces the ON DELETE CASCADE below.
        var cn = new SqliteConnection("Data Source=:memory:;Foreign Keys=True");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CdcBlog (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE CdcPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, " +
                "  FOREIGN KEY (BlogId) REFERENCES CdcBlog(Id) ON DELETE CASCADE);" +
                "INSERT INTO CdcBlog (Id) VALUES (1);" +
                "INSERT INTO CdcPost (Id, BlogId) VALUES (10, 1), (11, 1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Prime the cache with the child query.
        var before = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, before.Count);

        // Delete the blog WITHOUT loading its posts → the DB cascades DELETE to CdcPost.
        var blog = await ctx.Query<Blog>().FirstAsync(b => b.Id == 1);
        ctx.Remove(blog);
        await ctx.SaveChangesAsync();

        // Sanity: the posts really are gone from the database.
        using (var check = cn.CreateCommand())
        {
            check.CommandText = "SELECT COUNT(*) FROM CdcPost";
            Assert.Equal(0L, Convert.ToInt64(await check.ExecuteScalarAsync()));
        }

        // The Cacheable child query must NOT replay the cascade-deleted rows.
        var after = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Empty(after);   // BUG: returns the stale [10, 11]
    }
}
