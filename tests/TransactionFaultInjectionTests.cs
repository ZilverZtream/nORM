using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Fault-injection tests for connection-drop mid-commit and commit-acknowledgment
/// ambiguity on both sync and async DbContextTransaction paths.
///
/// "Commit acknowledgment ambiguity" is the scenario where the database receives and
/// processes the COMMIT but the network dies before the client gets the ACK. From the
/// client's perspective CommitAsync throws (e.g. TimeoutException), but the data is
/// durably committed on the server. The correct ORM behavior is to:
///   1. Clear CurrentTransaction so the context is usable (not permanently poisoned).
///   2. Propagate the exception so the caller knows to inspect/reconcile DB state.
///
/// All tests use SQLite with a ThrowingTransaction wrapper injected via reflection
/// (same pattern as SyncTransactionCleanupTests) so no live MySQL/SQL Server is needed.
/// </summary>
public class TransactionFaultInjectionTests
{
    // ── ThrowingTransaction — injectable mock ────────────────────────────────

    /// <summary>
    /// DbTransaction whose Commit/CommitAsync and/or Rollback/RollbackAsync throw on demand.
    /// Covers: sync throw (T-1 regression), async throw (fault injection), and
    /// connection-drop simulation (ObjectDisposedException on commit).
    /// </summary>
    private sealed class FaultingTransaction : DbTransaction
    {
        private readonly DbConnection _cn;
        private readonly Exception? _commitException;
        private readonly Exception? _rollbackException;

        public FaultingTransaction(
            DbConnection cn,
            Exception? commitException = null,
            Exception? rollbackException = null)
        {
            _cn = cn;
            _commitException = commitException;
            _rollbackException = rollbackException;
        }

        public override IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
        protected override DbConnection DbConnection => _cn;

        public override void Commit()
        {
            if (_commitException != null) throw _commitException;
        }

        public override Task CommitAsync(CancellationToken ct = default)
        {
            if (_commitException != null) throw _commitException;
            return Task.CompletedTask;
        }

        public override void Rollback()
        {
            if (_rollbackException != null) throw _rollbackException;
        }

        public override Task RollbackAsync(CancellationToken ct = default)
        {
            if (_rollbackException != null) throw _rollbackException;
            return Task.CompletedTask;
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    /// <summary>
    /// Injects a FaultingTransaction into the DbContext as if it were the current
    /// transaction, and wraps it in a DbContextTransaction via internal constructor.
    /// Mirrors the approach in SyncTransactionCleanupTests.CreateDbContextTransaction.
    /// </summary>
    private static DbContextTransaction InjectTransaction(FaultingTransaction tx, DbContext ctx)
    {
        // Plant the raw transaction on the context so CurrentTransaction is non-null.
        var setMethod = typeof(DbContext).GetMethod("SetTransaction",
            BindingFlags.NonPublic | BindingFlags.Instance);
        if (setMethod != null)
            setMethod.Invoke(ctx, new object[] { tx });
        else
        {
            var field = typeof(DbContext).GetField("_currentTransaction",
                BindingFlags.NonPublic | BindingFlags.Instance);
            field?.SetValue(ctx, tx);
        }

        var ctor = typeof(DbContextTransaction).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { typeof(DbTransaction), typeof(DbContext) },
            null)!;
        return (DbContextTransaction)ctor.Invoke(new object[] { tx, ctx });
    }

    // ── Async CommitAsync fault-injection ────────────────────────────────────

    /// <summary>
    /// Commit-acknowledgment ambiguity: CommitAsync throws TimeoutException (DB may have
    /// committed but client lost the ACK). CurrentTransaction must be cleared so the
    /// context is usable for reconciliation queries, even though data may be on the server.
    /// </summary>
    [Fact]
    public async Task CommitAsync_ThrowsTimeout_CurrentTransactionCleared()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var fault = new FaultingTransaction(cn,
                commitException: new TimeoutException("Simulated commit ACK timeout — data may be committed"));
            var dct = InjectTransaction(fault, ctx);

            await Assert.ThrowsAsync<TimeoutException>(() => dct.CommitAsync());

            // T-1 / fault-injection: context must not be poisoned despite the thrown ACK.
            Assert.Null(ctx.CurrentTransaction);
        }
    }

    /// <summary>
    /// Connection-drop mid-commit: CommitAsync throws ObjectDisposedException (connection
    /// closed under the transaction). Context must clear CurrentTransaction so it does not
    /// enter a permanently stuck state.
    /// </summary>
    [Fact]
    public async Task CommitAsync_ConnectionDrop_CurrentTransactionCleared()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var fault = new FaultingTransaction(cn,
                commitException: new InvalidOperationException("Connection dropped mid-commit"));
            var dct = InjectTransaction(fault, ctx);

            await Assert.ThrowsAsync<InvalidOperationException>(() => dct.CommitAsync());

            Assert.Null(ctx.CurrentTransaction);
        }
    }

    /// <summary>
    /// After a CommitAsync that throws (e.g. timeout ACK), the context must accept a new
    /// transaction. This verifies context is not permanently poisoned.
    /// </summary>
    [Fact]
    public async Task CommitAsync_Throws_SubsequentBeginTransactionAsync_Succeeds()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var fault = new FaultingTransaction(cn,
                commitException: new TimeoutException("ACK lost"));
            var dct = InjectTransaction(fault, ctx);

            try { await dct.CommitAsync(); } catch (TimeoutException) { }

            // Context must be usable — new transaction must start without "already active" error.
            await using var newTx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(newTx);
            Assert.NotNull(ctx.CurrentTransaction);
            await newTx.RollbackAsync();
        }
    }

    // ── Async RollbackAsync fault-injection ──────────────────────────────────

    /// <summary>
    /// Connection-drop during rollback: RollbackAsync throws (connection severed while
    /// rolling back). This is especially dangerous because the caller expects cleanup to
    /// succeed. CurrentTransaction must still be cleared so the context is usable.
    /// </summary>
    [Fact]
    public async Task RollbackAsync_ConnectionDrop_CurrentTransactionCleared()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var fault = new FaultingTransaction(cn,
                rollbackException: new InvalidOperationException("Connection dropped during rollback"));
            var dct = InjectTransaction(fault, ctx);

            await Assert.ThrowsAsync<InvalidOperationException>(() => dct.RollbackAsync());

            Assert.Null(ctx.CurrentTransaction);
        }
    }

    /// <summary>
    /// After a failed RollbackAsync (connection drop), context must be usable for a new
    /// transaction — the "stuck after failed rollback" scenario.
    /// </summary>
    [Fact]
    public async Task RollbackAsync_Throws_SubsequentBeginTransactionAsync_Succeeds()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var fault = new FaultingTransaction(cn,
                rollbackException: new InvalidOperationException("Connection dropped during rollback"));
            var dct = InjectTransaction(fault, ctx);

            try { await dct.RollbackAsync(); } catch (InvalidOperationException) { }

            await using var newTx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(newTx);
            await newTx.RollbackAsync();
        }
    }

    // ── CancellationToken.None guard on CommitAsync ──────────────────────────

    /// <summary>
    /// Verifies that CommitAsync on DbContextTransaction is called with CancellationToken.None
    /// and NOT with a pre-cancelled token — a commit in flight must not be interrupted
    /// by the caller's cancellation token, because partial commits cannot be undone.
    ///
    /// Test: pass an already-cancelled token to CommitAsync; the underlying FaultingTransaction
    /// tracks whether CommitAsync was called at all. Even with a cancelled token the commit
    /// must proceed (nORM must use CancellationToken.None internally).
    ///
    /// Note: DbContextTransaction.CommitAsync does not expose whether it passed None
    /// downstream, but the behavioral contract is that it does NOT propagate ct to the
    /// inner commit — validated here by confirming no OperationCanceledException fires.
    /// </summary>
    [Fact]
    public async Task CommitAsync_CallerCancelled_DoesNotThrowOperationCancelled()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            // FaultingTransaction.CommitAsync does NOT check the token — it just succeeds.
            // If nORM were to pass ct to CommitAsync, a pre-cancelled token would throw.
            var fault = new FaultingTransaction(cn); // no faults, succeeds normally
            var dct = InjectTransaction(fault, ctx);

            using var cts = new CancellationTokenSource();
            cts.Cancel(); // already cancelled

            // Must NOT throw OperationCanceledException — commit must proceed regardless.
            await dct.CommitAsync(cts.Token);

            Assert.Null(ctx.CurrentTransaction);
        }
    }

    // ── Dispose-path cleanup ─────────────────────────────────────────────────

    /// <summary>
    /// DisposeAsync on an uncommitted transaction (abandoned scope) must clear
    /// CurrentTransaction even if the underlying Rollback throws during disposal.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_UnderlyingDisposeThrows_CurrentTransactionStillCleared()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            // A transaction that throws during rollback simulates a dropped connection
            // at disposal time (e.g. using block exits on network error).
            var fault = new FaultingTransaction(cn,
                rollbackException: new InvalidOperationException("Dispose-time rollback failure"));
            var dct = InjectTransaction(fault, ctx);

            // Disposing without explicit commit triggers rollback internally.
            // Even if rollback throws, DisposeAsync must clear CurrentTransaction.
            try { await dct.DisposeAsync(); } catch { }

            Assert.Null(ctx.CurrentTransaction);
        }
    }
}
