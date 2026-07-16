using System;
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
/// Pins the retry strategy's interplay with explicit (user-owned) transactions, probed
/// differentially: a transient READ failure retries inside the still-valid transaction
/// (reads are side-effect-free), while a transient WRITE failure surfaces to the
/// transaction owner WITHOUT retry — the write is one statement of a logical unit nORM
/// cannot re-run, so the owner decides; the identical write OUTSIDE a transaction
/// retries and applies exactly once. Guards the neighbour class of the read-retry
/// dead-code kill and the write-retry duplicate invariants.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class RetryTransactionInteractionContractTests
{
    [Table("RetryTxn_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private sealed class InjectedTransientException : DbException
    {
        public InjectedTransientException() : base("injected transient failure") { }
    }

    private sealed class ArmableFailureInterceptor : BaseDbCommandInterceptor
    {
        private int _readFailures;
        private int _writeFailures;
        public int ReadAttempts;
        public int WriteAttempts;
        public ArmableFailureInterceptor() : base(NullLogger.Instance) { }
        public void ArmRead(int n) => Interlocked.Exchange(ref _readFailures, n);
        public void ArmWrite(int n) => Interlocked.Exchange(ref _writeFailures, n);

        private void TripRead(DbCommand command)
        {
            if (!command.CommandText.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)) return;
            Interlocked.Increment(ref ReadAttempts);
            if (Interlocked.Decrement(ref _readFailures) >= 0)
                throw new InjectedTransientException();
        }

        private void TripWrite(DbCommand command)
        {
            if (!command.CommandText.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase)
                && !command.CommandText.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)) return;
            Interlocked.Increment(ref WriteAttempts);
            if (Interlocked.Decrement(ref _writeFailures) >= 0)
                throw new InjectedTransientException();
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        {
            TripRead(command);
            TripWrite(command);   // batched writes read back generated keys via a reader
            return base.ReaderExecuting(command, context);
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            TripRead(command);
            TripWrite(command);
            return base.ReaderExecutingAsync(command, context, ct);
        }

        public override InterceptionResult<int> NonQueryExecuting(DbCommand command, DbContext context)
        {
            TripWrite(command);
            return base.NonQueryExecuting(command, context);
        }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            TripWrite(command);
            return base.NonQueryExecutingAsync(command, context, ct);
        }
    }

    private static (SqliteConnection, DbContext, ArmableFailureInterceptor) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE RetryTxn_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);" +
                "INSERT INTO RetryTxn_Row VALUES (1, 10), (2, 20);";
            cmd.ExecuteNonQuery();
        }
        var faults = new ArmableFailureInterceptor();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            RetryPolicy = new RetryPolicy { MaxRetries = 3, BaseDelay = TimeSpan.FromMilliseconds(1), ShouldRetry = static ex => ex is InjectedTransientException }
        };
        opts.CommandInterceptors.Add(faults);
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        return (cn, ctx, faults);
    }

    [Fact]
    public async Task Transient_read_failure_inside_an_explicit_transaction_retries_and_succeeds()
    {
        var (cn, ctx, faults) = Create();
        using var _cn = cn; await using var _ = ctx;

        await using var tx = await ctx.Database.BeginTransactionAsync();
        faults.ArmRead(1);
        // A read has no side effects and the injected fault never reached the server,
        // so the transaction is still valid — the retry runs INSIDE it and succeeds.
        var rows = await ctx.Query<Row>().Where(r => r.V >= 10).ToListAsync();
        await tx.RollbackAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(2, faults.ReadAttempts);
    }

    [Fact]
    public async Task Transient_write_failure_inside_an_explicit_transaction_surfaces_without_retry()
    {
        var (cn, ctx, faults) = Create();
        using var _cn = cn; await using var _ = ctx;

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            faults.ArmWrite(1);
            ctx.Add(new Row { Id = 3, V = 30 });
            // Inside a USER-owned transaction the write is one statement of a larger
            // logical unit nORM cannot re-run, so the failure surfaces to the owner
            // instead of retrying — the owner decides to roll back.
            await Assert.ThrowsAsync<InjectedTransientException>(() => ctx.SaveChangesAsync());
            Assert.Equal(1, faults.WriteAttempts);
            await tx.RollbackAsync();
        }

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM RetryTxn_Row WHERE Id = 3";
        Assert.Equal(0, Convert.ToInt32(check.ExecuteScalar()));
    }

    [Fact]
    public async Task The_same_transient_write_failure_outside_a_transaction_retries_and_applies_exactly_once()
    {
        var (cn, ctx, faults) = Create();
        using var _cn = cn; await using var _ = ctx;

        faults.ArmWrite(1);
        ctx.Add(new Row { Id = 3, V = 30 });
        await ctx.SaveChangesAsync();   // nORM owns the write unit here, so it retries

        Assert.True(faults.WriteAttempts >= 2, $"expected a retry, saw {faults.WriteAttempts} attempt(s)");
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM RetryTxn_Row WHERE Id = 3";
        Assert.Equal(1, Convert.ToInt32(check.ExecuteScalar()));
    }
}
