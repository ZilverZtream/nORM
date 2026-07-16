using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contracts for the retry strategy's interplay with the read pipeline and the result cache
/// (resilience matrix cells). The retry strategy wraps the ENTIRE cached execution path, so a
/// transient first-attempt failure must retry to a correct result and populate the cache exactly
/// once - never a partial or empty entry from the failed attempt. Retried tracked reads stay
/// idempotent (one tracked instance, correct aggregates), and without a retry policy a
/// non-transient failure surfaces loudly with nothing cached.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ResilienceRetryCacheContractTests
{
    [Table("RetryCache_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private sealed class InjectedTransientException : DbException
    {
        public InjectedTransientException() : base("injected transient failure") { }
    }

    /// <summary>Throws on the first N SELECT executions, then succeeds.</summary>
    private sealed class ArmableReadFailureInterceptor : BaseDbCommandInterceptor
    {
        private int _failuresRemaining;
        public ArmableReadFailureInterceptor() : base(NullLogger.Instance) { }
        public void Arm(int failures) => Interlocked.Exchange(ref _failuresRemaining, failures);

        private void TripIfArmed(DbCommand command)
        {
            if (command.CommandText.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                && Interlocked.Decrement(ref _failuresRemaining) >= 0)
                throw new InjectedTransientException();
        }

        // SQLite prefers sync reader execution, so faults must trip on BOTH hook variants -
        // an async-only override never fires there.
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            TripIfArmed(command);
            return base.ReaderExecuting(command, context);
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            TripIfArmed(command);
            return base.ReaderExecutingAsync(command, context, ct);
        }
    }

    private sealed class CountingCache : IDbCacheProvider, IDisposable
    {
        private readonly NormMemoryCacheProvider _inner = new();
        public int Sets;
        public bool TryGet<T>(string key, out T? value) => _inner.TryGet(key, out value);
        public void Set<T>(string key, T value, TimeSpan expiration, IEnumerable<string> tags)
        {
            Interlocked.Increment(ref Sets);
            _inner.Set(key, value, expiration, tags);
        }
        public void InvalidateTag(string tag) => _inner.InvalidateTag(tag);
        public void Dispose() => _inner.Dispose();
    }

    private static (SqliteConnection Cn, DbContext Ctx, CountingCache Cache, ArmableReadFailureInterceptor Faults) Create(bool retry)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE RetryCache_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);" +
                "INSERT INTO RetryCache_Row VALUES (1, 10), (2, 20);";
            cmd.ExecuteNonQuery();
        }
        var cache = new CountingCache();
        var faults = new ArmableReadFailureInterceptor();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            CacheProvider = cache,
            RetryPolicy = retry ? new RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.FromMilliseconds(1), ShouldRetry = static ex => ex is InjectedTransientException } : null
        };
        opts.CommandInterceptors.Add(faults);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx, cache, faults);
    }

    [Fact]
    public async Task Transient_first_attempt_retries_and_populates_the_cache_exactly_once()
    {
        var (cn, ctx, cache, faults) = Create(retry: true);
        using var _cn = cn; using var _c = cache; await using var _ = ctx;

        faults.Arm(1);
        var rows = await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, cache.Sets);   // the failed attempt cached nothing

        // The cached entry serves correctly afterwards without another Set.
        var again = await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, again.Count);
        Assert.Equal(1, cache.Sets);
    }

    [Fact]
    public async Task Retried_tracked_read_stays_idempotent()
    {
        var (cn, ctx, cache, faults) = Create(retry: true);
        using var _cn = cn; using var _c = cache; await using var _ = ctx;

        faults.Arm(1);
        var rows = await ctx.Query<Row>().Where(r => r.V >= 10).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, ctx.ChangeTracker.Entries.Count(e => e.Entity is Row));

        faults.Arm(1);
        Assert.Equal(2, await ctx.Query<Row>().CountAsync());
    }

    [Fact]
    public async Task Exhausted_retries_surface_loudly_and_cache_nothing()
    {
        var (cn, ctx, cache, faults) = Create(retry: true);
        using var _cn = cn; using var _c = cache; await using var _ = ctx;

        faults.Arm(int.MaxValue);
        await Assert.ThrowsAnyAsync<NormException>(() =>
            ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync());
        Assert.Equal(0, cache.Sets);
    }

    [Fact]
    public async Task Without_a_retry_policy_a_failure_is_loud_and_caches_nothing()
    {
        var (cn, ctx, cache, faults) = Create(retry: false);
        using var _cn = cn; using var _c = cache; await using var _ = ctx;

        faults.Arm(1);
        await Assert.ThrowsAnyAsync<Exception>(() =>
            ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync());
        Assert.Equal(0, cache.Sets);

        // The next read (fault disarmed) succeeds and caches normally.
        var rows = await ((INormQueryable<Row>)ctx.Query<Row>()).AsNoTracking().Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, cache.Sets);
    }
}
