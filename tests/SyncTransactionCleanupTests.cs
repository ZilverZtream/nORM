using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// T-1: Verifies that DbContextTransaction.Commit() and Rollback() (sync) always clear
/// the context's CurrentTransaction reference via Dispose(), even when the underlying
/// operation throws.
///
/// Root bug: sync Commit() and Rollback() called Dispose() only after the operation
/// succeeded. If Commit() or Rollback() threw, Dispose() was skipped — leaving
/// CurrentTransaction non-null and the context permanently poisoned (subsequent
/// BeginTransactionAsync calls would behave incorrectly).
///
/// Fix: both sync methods now use try/finally, matching the async versions which
/// already had try/finally with DisposeAsync.
/// </summary>
public class SyncTransactionCleanupTests
{
    // ── Fake transaction that throws on Commit/Rollback ──────────────────────

    private sealed class ThrowingTransaction : DbTransaction
    {
        private readonly DbConnection _cn;
        private readonly bool _throwOnCommit;
        private readonly bool _throwOnRollback;

        public ThrowingTransaction(DbConnection cn, bool throwOnCommit = false, bool throwOnRollback = false)
        {
            _cn = cn;
            _throwOnCommit = throwOnCommit;
            _throwOnRollback = throwOnRollback;
        }

        public override IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
        protected override DbConnection DbConnection => _cn;

        public override void Commit()
        {
            if (_throwOnCommit)
                throw new InvalidOperationException("Simulated commit failure");
        }

        public override void Rollback()
        {
            if (_throwOnRollback)
                throw new InvalidOperationException("Simulated rollback failure");
        }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── T-1: Sync Commit throws — CurrentTransaction must be cleared ─────────

    /// <summary>
    /// T-1 regression: When sync Commit() throws, Dispose() must still run so that
    /// the context's CurrentTransaction is cleared. Before the fix, Dispose() was only
    /// called when Commit() returned normally; an exception left CurrentTransaction set.
    /// </summary>
    [Fact]
    public void Commit_Throws_CurrentTransactionIsCleared()
    {
        var (cn, ctx) = CreateContext();
        using (cn) using (ctx)
        {
            // Wrap a throwing transaction in DbContextTransaction directly via reflection.
            var throwingTx = new ThrowingTransaction(cn, throwOnCommit: true);
            var dct = CreateDbContextTransaction(throwingTx, ctx);

            // Commit must throw the simulated exception.
            Assert.Throws<InvalidOperationException>(() => dct.Commit());

            // T-1 fix: CurrentTransaction must be null — Dispose() ran in the finally block.
            // Before the fix, CurrentTransaction remained set after the throw.
            Assert.Null(ctx.CurrentTransaction);
        }
    }

    /// <summary>
    /// T-1: Ensure that after a throwing Commit(), starting a new transaction does not
    /// fail with "already active transaction" or similar poisoning.
    /// </summary>
    [Fact]
    public async Task Commit_Throws_SubsequentBeginTransactionAsync_Succeeds()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var throwingTx = new ThrowingTransaction(cn, throwOnCommit: true);
            var dct = CreateDbContextTransaction(throwingTx, ctx);

            try { dct.Commit(); } catch (InvalidOperationException) { }

            // T-1 fix: context must not be poisoned — a new transaction must start cleanly.
            await using var newTx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(newTx);
            Assert.NotNull(ctx.CurrentTransaction);
            await newTx.RollbackAsync();
        }
    }

    // ── T-1: Sync Rollback throws — CurrentTransaction must be cleared ───────

    /// <summary>
    /// T-1 regression: When sync Rollback() throws, Dispose() must still run.
    /// </summary>
    [Fact]
    public void Rollback_Throws_CurrentTransactionIsCleared()
    {
        var (cn, ctx) = CreateContext();
        using (cn) using (ctx)
        {
            var throwingTx = new ThrowingTransaction(cn, throwOnRollback: true);
            var dct = CreateDbContextTransaction(throwingTx, ctx);

            Assert.Throws<InvalidOperationException>(() => dct.Rollback());

            // T-1 fix: CurrentTransaction cleared even after throwing Rollback().
            Assert.Null(ctx.CurrentTransaction);
        }
    }

    /// <summary>
    /// T-1: After a throwing Rollback(), the context must accept a new transaction.
    /// </summary>
    [Fact]
    public async Task Rollback_Throws_SubsequentBeginTransactionAsync_Succeeds()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var throwingTx = new ThrowingTransaction(cn, throwOnRollback: true);
            var dct = CreateDbContextTransaction(throwingTx, ctx);

            try { dct.Rollback(); } catch (InvalidOperationException) { }

            await using var newTx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(newTx);
            await newTx.RollbackAsync();
        }
    }

    // ── Happy path: sync Commit/Rollback without exception ───────────────────

    /// <summary>
    /// T-1: Normal sync Commit (no exception) must still clear CurrentTransaction.
    /// Verifies the try/finally doesn't interfere with the success path.
    /// </summary>
    [Fact]
    public async Task Commit_Succeeds_CurrentTransactionIsCleared()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "CREATE TABLE T1 (Id INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();

            await using var tx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(ctx.CurrentTransaction);

            // Commit via the sync path
            tx.Commit();

            Assert.Null(ctx.CurrentTransaction);
        }
    }

    /// <summary>
    /// T-1: Normal sync Rollback must clear CurrentTransaction (success path guard).
    /// </summary>
    [Fact]
    public async Task Rollback_Succeeds_CurrentTransactionIsCleared()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            await using var tx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(ctx.CurrentTransaction);

            tx.Rollback();

            Assert.Null(ctx.CurrentTransaction);
        }
    }

    // ── Helper: bypass Database.BeginTransactionAsync so we can inject ThrowingTransaction

    private static DbContextTransaction CreateDbContextTransaction(DbTransaction tx, DbContext ctx)
    {
        // Set CurrentTransaction on the context so it's non-null, then wrap it.
        // Use the internal SetTransaction / ClearTransaction surface via reflection
        // to plant our throwing transaction.
        var setMethod = typeof(DbContext).GetMethod("SetTransaction",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        if (setMethod != null)
        {
            setMethod.Invoke(ctx, new object[] { tx });
        }
        else
        {
            // Fallback: directly set _currentTransaction field
            var field = typeof(DbContext).GetField("_currentTransaction",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            field?.SetValue(ctx, tx);
        }

        // Construct DbContextTransaction via its internal constructor
        var ctor = typeof(DbContextTransaction).GetConstructor(
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
            null,
            new[] { typeof(DbTransaction), typeof(DbContext) },
            null)!;
        return (DbContextTransaction)ctor.Invoke(new object[] { tx, ctx });
    }
}
