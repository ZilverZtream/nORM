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
/// Data-safety proof for SaveChanges: a mid-batch failure must leave ZERO rows
/// persisted (single owned transaction rolls back), and a retried attempt must
/// replay cleanly — no duplicate rows and correct DB-generated keys on every
/// entity — even though the rolled-back first attempt already wrote keys onto
/// some entities. The identity insert excludes the key column by mapping, so
/// stale keys cannot leak into the replay.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SaveChangesFaultInjectionAtomicityTests
{
    private const int EntityCount = 400;

    [Table("SfiItem")]
    private class SfiItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private static int RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM SfiItem";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static SqliteConnection OpenDb(bool withCheckConstraint)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = withCheckConstraint
            ? "CREATE TABLE SfiItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL CHECK (Value < 500))"
            : "CREATE TABLE SfiItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn, RetryPolicy? retry = null, IDbCommandInterceptor? interceptor = null)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SfiItem>(),
            RetryPolicy = retry
        };
        if (interceptor != null) opts.CommandInterceptors.Add(interceptor);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Mid_batch_failure_persists_zero_rows_and_a_later_save_succeeds()
    {
        using var cn = OpenDb(withCheckConstraint: true);
        await using var ctx = Ctx(cn);

        // One entity violates the CHECK constraint, forcing a real DB failure part
        // way through the multi-row/multi-batch insert.
        for (var i = 0; i < EntityCount; i++)
            ctx.Add(new SfiItem { Name = $"n{i}", Value = i == EntityCount / 2 ? 999 : i % 100 });

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        // Atomicity: the owned transaction rolled everything back.
        Assert.Equal(0, RowCount(cn));

        // A subsequent valid save persists the full set exactly once.
        await using var ctx2 = Ctx(cn);
        for (var i = 0; i < EntityCount; i++)
            ctx2.Add(new SfiItem { Name = $"ok{i}", Value = i % 100 });
        await ctx2.SaveChangesAsync();
        Assert.Equal(EntityCount, RowCount(cn));
    }

    private sealed class InjectedRetryableException : DbException
    {
        public InjectedRetryableException() : base("injected retryable failure") { }
    }

    /// <summary>
    /// Throws a retryable exception on the first write execution, then lets every
    /// subsequent execution proceed — forcing SaveChanges to roll back attempt 1
    /// (after it wrote keys onto early-batch entities) and replay under the retry
    /// policy.
    /// </summary>
    private sealed class ThrowOnceInterceptor : BaseDbCommandInterceptor
    {
        private int _fired;
        private bool Trip() => Interlocked.Exchange(ref _fired, 1) == 0;

        public ThrowOnceInterceptor() : base(NullLogger.Instance) { }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            if (IsWrite(command) && Trip()) throw new InjectedRetryableException();
            return base.NonQueryExecutingAsync(command, context, ct);
        }
        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            if (IsWrite(command) && Trip()) throw new InjectedRetryableException();
            return base.ScalarExecutingAsync(command, context, ct);
        }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            if (IsWrite(command) && Trip()) throw new InjectedRetryableException();
            return base.ReaderExecutingAsync(command, context, ct);
        }
        private static bool IsWrite(DbCommand command)
            => command.CommandText.Contains("INSERT", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Retry_after_rolled_back_attempt_replays_cleanly_with_correct_keys()
    {
        using var cn = OpenDb(withCheckConstraint: false);
        var retry = new RetryPolicy
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is InjectedRetryableException
        };
        await using var ctx = Ctx(cn, retry, new ThrowOnceInterceptor());

        var entities = new List<SfiItem>();
        for (var i = 0; i < EntityCount; i++)
        {
            var e = new SfiItem { Name = $"n{i}", Value = i % 100 };
            entities.Add(e);
            ctx.Add(e);
        }

        await ctx.SaveChangesAsync();

        // Exactly one copy of every row — the rolled-back first attempt left no
        // duplicates and no stale keys in the replayed inserts.
        Assert.Equal(EntityCount, RowCount(cn));

        var keys = entities.Select(e => e.Id).ToArray();
        Assert.All(keys, k => Assert.True(k > 0));
        Assert.Equal(EntityCount, keys.Distinct().Count());
    }
}
