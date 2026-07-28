using System;
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
/// A set-based (ExecuteDelete) or bulk delete that triggers a DB ON DELETE CASCADE must invalidate the
/// cascade-dependent child tables' result cache, not only the target/root table. SaveChanges already does this
/// (AddCascadeDependentTables); ExecuteDelete/BulkDelete invalidated only the root table, so a Cacheable query
/// over the cascade-deleted child table kept replaying the deleted rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ExecuteDeleteCascadeCacheInvalidationTests
{
    [Table("EdcBlog")]
    public class Blog { [Key] public int Id { get; set; } public List<Post> Posts { get; set; } = new(); }
    [Table("EdcPost")]
    public class Post { [Key] public int Id { get; set; } public int BlogId { get; set; } }

    private static DbContext Ctx(out SqliteConnection cn, NormMemoryCacheProvider cache)
    {
        cn = new SqliteConnection("Data Source=:memory:;Foreign Keys=True");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EdcBlog (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EdcPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, FOREIGN KEY (BlogId) REFERENCES EdcBlog(Id) ON DELETE CASCADE);" +
                "INSERT INTO EdcBlog (Id) VALUES (1);" +
                "INSERT INTO EdcPost (Id, BlogId) VALUES (10,1),(11,1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task ExecuteDelete_cascade_invalidates_child_table_cache()
    {
        using var cache = new NormMemoryCacheProvider();
        await using var ctx = Ctx(out var cn, cache);
        using var _cn = cn;

        var before = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, before.Count);

        await ctx.Query<Blog>().Where(b => b.Id == 1).ExecuteDeleteAsync();

        var after = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Empty(after);   // cascade-deleted from the DB — cache must not serve stale rows
    }

    [Fact]
    public async Task BulkDelete_cascade_invalidates_child_table_cache()
    {
        using var cache = new NormMemoryCacheProvider();
        await using var ctx = Ctx(out var cn, cache);
        using var _cn = cn;

        var before = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, before.Count);

        await ctx.BulkDeleteAsync(new[] { new Blog { Id = 1 } });

        var after = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Empty(after);
    }
}
