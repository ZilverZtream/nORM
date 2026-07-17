using System;
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
/// Pins that the result-cache key preserves sub-second precision on temporal parameters.
/// Two Cacheable queries that differ only by a sub-second DateTime must NOT collide on one
/// cache key — before the fix both rendered through the general format (no fractional
/// seconds), so the second query served the first's cached rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CacheKeyTemporalPrecisionContractTests
{
    [Table("CktRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public DateTime Ts { get; set; }
    }

    [Fact]
    public async Task Cacheable_queries_differing_by_sub_second_timestamp_do_not_collide()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CktRow (Id INTEGER PRIMARY KEY, Ts TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using var cache = new NormMemoryCacheProvider(null);
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Two timestamps 500 ms apart — identical under the general ("G") format that drops
        // fractional seconds, distinct under round-trip. Insert through nORM so the stored form
        // matches nORM's own parameter binding.
        var t1 = new DateTime(2024, 1, 1, 10, 0, 0, 0, DateTimeKind.Utc);
        var t2 = t1.AddMilliseconds(500);
        ctx.Add(new Row { Id = 1, Ts = t1 });
        ctx.Add(new Row { Id = 2, Ts = t2 });
        await ctx.SaveChangesAsync();

        var expiry = TimeSpan.FromMinutes(5);

        // First Cacheable query warms the cache under t1's key.
        var first = (await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking()
                .Where(r => r.Ts == t1).Cacheable(expiry).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1 }, first);

        // Second Cacheable query differs ONLY by the sub-second timestamp: it must return row 2,
        // not the cached row-1 result.
        var second = (await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking()
                .Where(r => r.Ts == t2).Cacheable(expiry).ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2 }, second);
    }
}
