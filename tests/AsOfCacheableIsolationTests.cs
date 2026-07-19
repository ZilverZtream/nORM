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
/// Two cacheable queries that differ only by their AsOf timestamp must not collide on one cache key — the
/// timestamp is bound as a parameter that participates in the result-cache key, so each era gets its own
/// entry. Without this, a cached historical read would be served for a different timestamp (wrong era). This
/// complements the DateTime-filter precision test (<see cref="CacheKeyTemporalPrecisionContractTests"/>),
/// which covers a <c>Where</c> DateTime parameter rather than <c>AsOf</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsOfCacheableIsolationTests
{
    [Table("AocRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static async Task<DateTime> ServerNow(SqliteConnection cn)
    {
        using var c = cn.CreateCommand();
        c.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f','now')";
        return DateTime.SpecifyKind(DateTime.Parse((string)(await c.ExecuteScalarAsync())!, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    [Fact]
    public async Task cacheable_reads_at_two_as_of_timestamps_do_not_collide()
    {
        using var cache = new NormMemoryCacheProvider(null);
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AocRow (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id)
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        ctx.Add(new Row { Id = 1, Val = 10 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNow(cn);                       // era: Val = 10
        await Task.Delay(60);
        var r = ctx.Find<Row>(1)!; r.Val = 100; await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t2 = await ServerNow(cn);                       // era: Val = 100
        var expiry = TimeSpan.FromMinutes(5);

        // Warm the cache at t1, then read at t2 — the second must NOT return the cached t1 value.
        var atT1 = (await ((INormQueryable<Row>)ctx.Query<Row>()).AsOf(t1).Cacheable(expiry).ToListAsync()).Single().Val;
        var atT2 = (await ((INormQueryable<Row>)ctx.Query<Row>()).AsOf(t2).Cacheable(expiry).ToListAsync()).Single().Val;
        Assert.Equal(10, atT1);
        Assert.Equal(100, atT2);
    }
}
