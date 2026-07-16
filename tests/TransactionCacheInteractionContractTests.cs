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

/// <summary>
/// Contract: the result cache never trades transactional correctness for hits (caching matrix
/// cell). A Cacheable query executed INSIDE a transaction observes the transaction's own
/// uncommitted writes - if that result were cached, a ROLLBACK would leave the cache serving
/// never-committed state to later readers. And a cached entry produced BEFORE a transaction must
/// not survive the transaction's committed writes as a stale serve. Both directions are probed
/// against a fresh raw-SQL oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TransactionCacheInteractionContractTests
{
    [Table("TxnCache_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx, NormMemoryCacheProvider Cache) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TxnCache_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);" +
                "INSERT INTO TxnCache_Row VALUES (1, 10);";
            cmd.ExecuteNonQuery();
        }
        var cache = new NormMemoryCacheProvider();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            CacheProvider = cache
        });
        return (cn, ctx, cache);
    }

    [Fact]
    public async Task Rolled_back_writes_never_reach_later_readers_through_the_cache()
    {
        var (cn, ctx, cache) = Create();
        using var _cn = cn;
        using var _cache = cache;
        await using var _ = ctx;

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new Row { Id = 2, V = 20 });
            await ctx.SaveChangesAsync();

            // A cacheable read inside the transaction sees the uncommitted row.
            var inside = await ((INormQueryable<Row>)ctx.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
            Assert.Equal(2, inside.Count);

            await tx.RollbackAsync();
        }

        // Raw truth: the rollback removed the row.
        using (var check = cn.CreateCommand())
        {
            check.CommandText = "SELECT COUNT(*) FROM TxnCache_Row;";
            Assert.Equal(1L, check.ExecuteScalar());
        }

        // The SAME cacheable query after the rollback must reflect committed state - the
        // in-transaction result must not have poisoned the cache.
        var after = await ((INormQueryable<Row>)ctx.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        var only = Assert.Single(after);
        Assert.Equal(1, only.Id);
    }

    [Fact]
    public async Task Committed_transaction_writes_invalidate_a_pre_transaction_cache_entry()
    {
        var (cn, ctx, cache) = Create();
        using var _cn = cn;
        using var _cache = cache;
        await using var _ = ctx;

        // Populate the cache before the transaction.
        var before = await ((INormQueryable<Row>)ctx.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Single(before);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new Row { Id = 2, V = 20 });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        }

        // The committed write must not be hidden by the pre-transaction entry.
        var after = await ((INormQueryable<Row>)ctx.Query<Row>()).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, after.Count);
    }
}
