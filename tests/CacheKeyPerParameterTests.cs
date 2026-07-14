using System;
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

[Trait("Category", "Fast")]
public class CacheKeyPerParameterTests
{
    [Table("CkpRow")]
    public class Row { [Key] public int Id { get; set; } public int Val { get; set; } }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(NormMemoryCacheProvider cache)
    {
        var keeper = new SqliteConnection($"Data Source=file:ckp_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CkpRow (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
            for (var i = 1; i <= 20; i++)
            {
                using var ins = keeper.CreateCommand();
                ins.CommandText = $"INSERT INTO CkpRow VALUES ({i}, {i})";
                ins.ExecuteNonQuery();
            }
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                CacheProvider = cache,
                OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id)
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    [Fact]
    public async Task Cacheable_query_keys_by_closure_parameter_value()
    {
        using var cache = new NormMemoryCacheProvider();
        var (keeper, make) = Setup(cache);
        using var _ = keeper;
        await using var ctx = make();

        // Same SQL shape, different closure values — each must get its own cached result.
        int CountAbove(int threshold)
        {
            int t = threshold; // closure capture
            var rows = ((INormQueryable<Row>)ctx.Query<Row>())
                .AsNoTracking().Where(r => r.Val > t)
                .Cacheable(TimeSpan.FromMinutes(5))
                .ToList();
            return rows.Count;
        }

        // Interleave to stress the cache: warm 5, then 15, then re-check 5 (must not be poisoned by 15).
        Assert.Equal(15, CountAbove(5));   // Val 6..20
        Assert.Equal(5, CountAbove(15));   // Val 16..20
        Assert.Equal(15, CountAbove(5));   // must still be 15, not the cached-for-15 result
        Assert.Equal(20, CountAbove(0));   // all
        Assert.Equal(0, CountAbove(20));   // none
        Assert.Equal(5, CountAbove(15));   // re-check the middle value
        await Task.CompletedTask;
    }
}
