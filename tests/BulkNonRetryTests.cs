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
/// Bulk, stored-procedure, and temporal writes are non-idempotent and have no commit barrier the retry
/// strategy can observe, so under a configured retry policy they must execute ONCE and fail fast rather
/// than replay a possibly-committed operation and duplicate rows. The tracked single-entity/SaveChanges
/// paths, which thread a real commit-attempted flag, must still retry a transient pre-commit fault.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkNonRetryTests
{
    [Table("BnrRow")]
    public class BnrRow
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class InjectedRetryableException : DbException
    {
        public InjectedRetryableException() : base("injected retryable failure") { }
    }

    private sealed class ArmableWriteFailureInterceptor : BaseDbCommandInterceptor
    {
        private int _failuresRemaining;
        public ArmableWriteFailureInterceptor() : base(NullLogger.Instance) { }
        public void Arm(int failures) => Interlocked.Exchange(ref _failuresRemaining, failures);

        private void TripIfArmed(DbCommand command)
        {
            if (!IsWrite(command)) return;
            if (Interlocked.Decrement(ref _failuresRemaining) >= 0)
                throw new InjectedRetryableException();
            Interlocked.Increment(ref _failuresRemaining);
        }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { TripIfArmed(command); return base.NonQueryExecutingAsync(command, context, ct); }
        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { TripIfArmed(command); return base.ScalarExecutingAsync(command, context, ct); }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { TripIfArmed(command); return base.ReaderExecutingAsync(command, context, ct); }

        private static bool IsWrite(DbCommand command)
            => command.CommandText.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
               || command.CommandText.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
               || command.CommandText.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase);
    }

    private static (SqliteConnection cn, DbContext ctx) Create(ArmableWriteFailureInterceptor interceptor)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BnrRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = ex => ex is InjectedRetryableException
            }
        };
        opts.CommandInterceptors.Add(interceptor);
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Tracked_save_still_retries_a_transient_write_fault()
    {
        var interceptor = new ArmableWriteFailureInterceptor();
        var (cn, ctx) = Create(interceptor);
        using var _cn = cn;
        await using var _ctx = ctx;

        interceptor.Arm(1); // one transient failure, within the retry budget
        ctx.Add(new BnrRow { Name = "x" });
        await ctx.SaveChangesAsync(); // must transparently retry and land the row

        Assert.Single(await ctx.Query<BnrRow>().ToListAsync());
    }

    [Fact]
    public async Task Bulk_insert_is_not_retried_and_fails_fast_on_a_transient_write_fault()
    {
        var interceptor = new ArmableWriteFailureInterceptor();
        var (cn, ctx) = Create(interceptor);
        using var _cn = cn;
        await using var _ctx = ctx;

        interceptor.Arm(1); // one transient failure
        var rows = new[] { new BnrRow { Name = "a" }, new BnrRow { Name = "b" }, new BnrRow { Name = "c" } };

        // A non-idempotent bulk insert must NOT be retried past the fault (retrying could duplicate
        // rows if the fault occurred at/after commit), so it fails fast.
        await Assert.ThrowsAnyAsync<Exception>(() => ctx.BulkInsertAsync(rows));

        // The failed insert rolled back; nothing was committed.
        Assert.Empty(await ctx.Query<BnrRow>().ToListAsync());
    }
}
