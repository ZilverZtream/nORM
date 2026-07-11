using System;
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
/// Set-based bulk writes (ExecuteDeleteAsync / ExecuteUpdateAsync) must invalidate
/// the result cache like every other write path (SaveChanges and the Bulk* ops do).
/// Otherwise a Cacheable() query keeps returning rows the bulk write already
/// deleted or changed — a stale read of persisted data.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ExecuteDeleteUpdateCacheInvalidationTests
{
    [Table("EducItem")]
    private class EducItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContextOptions Opts, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EducItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL);" +
                "INSERT INTO EducItem (Value) VALUES (10),(20),(30);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<EducItem>(),
            CacheProvider = new NormMemoryCacheProvider()
        };
        return (cn, opts, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Execute_delete_invalidates_the_result_cache()
    {
        var (cn, opts, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Prime the cache.
        var before = await ctx.Query<EducItem>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(3, before.Count);

        // Set-based delete of two rows.
        var deleted = await ctx.Query<EducItem>().Where(x => x.Value > 15).ExecuteDeleteAsync();
        Assert.Equal(2, deleted);

        // Read through a fresh context (fresh identity map, shared cache) so the
        // result reflects cache-or-DB, not tracked instances.
        await using var reader = new DbContext(cn, new SqliteProvider(), opts);
        var after = await reader.Query<EducItem>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(after);
        Assert.Equal(10, after[0].Value);
    }

    [Fact]
    public async Task Execute_update_invalidates_the_result_cache()
    {
        var (cn, opts, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var before = await ctx.Query<EducItem>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(3, before.Count);

        // Set-based update: bump every value by 100.
        var updated = await ctx.Query<EducItem>().ExecuteUpdateAsync(s => s.SetProperty(x => x.Value, x => x.Value + 100));
        Assert.Equal(3, updated);

        // ExecuteUpdate is set-based and bypasses the ChangeTracker (like EF Core), so
        // read through a fresh context to observe DB values rather than tracked instances.
        // If the result cache was not invalidated, this fresh read still hits the stale
        // cached list and fails.
        await using var reader = new DbContext(cn, new SqliteProvider(), opts);
        var after = await reader.Query<EducItem>().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(new[] { 110, 120, 130 }, after.Select(x => x.Value).OrderBy(v => v).ToArray());
    }
}
