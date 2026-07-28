using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// The direct active-record DeleteAsync must cascade-invalidate the result cache for every table a DB
/// ON DELETE CASCADE removes rows from — exactly as SaveChanges / ExecuteDelete / BulkDelete do. It only
/// tagged the deleted entity's own table, so a Cacheable() query over a cascade-removed child table kept
/// serving the deleted rows (a silent stale read).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DirectDeleteCascadeCacheInvalidationTests
{
    [Table("DdcBlog")] public sealed class Blog { [Key] public int Id { get; set; } public List<Post> Posts { get; set; } = new(); }
    [Table("DdcPost")] public sealed class Post { [Key] public int Id { get; set; } public int BlogId { get; set; } }

    [Fact]
    public async Task Direct_DeleteAsync_cascade_invalidates_child_table_cache()
    {
        using var cache = new NormMemoryCacheProvider();
        var cn = new SqliteConnection("Data Source=:memory:;Foreign Keys=True");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE DdcBlog (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE DdcPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, " +
                "  FOREIGN KEY (BlogId) REFERENCES DdcBlog(Id) ON DELETE CASCADE);" +
                "INSERT INTO DdcBlog (Id) VALUES (1);" +
                "INSERT INTO DdcPost (Id, BlogId) VALUES (10, 1), (11, 1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var before = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, before.Count);                       // cache primed

        await ctx.DeleteAsync(new Blog { Id = 1 });          // direct active-record delete; DB cascades to DdcPost

        using (var check = cn.CreateCommand())
        {
            check.CommandText = "SELECT COUNT(*) FROM DdcPost";
            Assert.Equal(0L, Convert.ToInt64(await check.ExecuteScalarAsync()));   // rows really gone
        }

        var after = await ctx.Query<Post>().Where(p => p.BlogId == 1).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Empty(after);   // was stale [10, 11]
    }
}
