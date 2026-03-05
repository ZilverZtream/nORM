using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// CT-1/TX-1: Verifies that internally-created DbTransaction objects are always disposed
/// after each write operation (Insert, Update, Delete), even when the operation fails.
/// This prevents connection-level resource leaks (lock memory, log space) under load.
/// </summary>
public class TransactionLifecycleTests
{
    [Table("TxLifecycleItem")]
    private class TxLifecycleItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    // ── Tracking wrapper ─────────────────────────────────────────────────────

    /// <summary>
    /// Wraps a SqliteConnection to intercept BeginTransactionAsync and track disposal of
    /// every created transaction.
    /// </summary>
    private sealed class TrackingConnection : DbConnection
    {
        private readonly SqliteConnection _inner;
        public List<bool> TransactionWasDisposed { get; } = new();

        public TrackingConnection(string connectionString)
        {
            _inner = new SqliteConnection(connectionString);
        }

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => _inner.ConnectionString;
            set => _inner.ConnectionString = value!;
        }

        public override string Database => _inner.Database;
        public override string DataSource => _inner.DataSource;
        public override string ServerVersion => _inner.ServerVersion;
        public override ConnectionState State => _inner.State;

        public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
        public override void Close() => _inner.Close();
        public override void Open() => _inner.Open();
        public override Task OpenAsync(CancellationToken cancellationToken) => _inner.OpenAsync(cancellationToken);

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            var inner = _inner.BeginTransaction(isolationLevel);
            int idx = TransactionWasDisposed.Count;
            TransactionWasDisposed.Add(false);
            return new TrackingTransaction(inner, this, () => TransactionWasDisposed[idx] = true);
        }

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
            IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            var inner = await _inner.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            int idx = TransactionWasDisposed.Count;
            TransactionWasDisposed.Add(false);
            return new TrackingTransaction((SqliteTransaction)inner, this, () => TransactionWasDisposed[idx] = true);
        }

        protected override DbCommand CreateDbCommand()
        {
            var innerCmd = _inner.CreateCommand();
            return new PassthroughCommand(innerCmd, this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed class TrackingTransaction : DbTransaction
    {
        private readonly SqliteTransaction _inner;
        private readonly Action _onDispose;
        private readonly DbConnection _connection;

        public TrackingTransaction(SqliteTransaction inner, DbConnection connection, Action onDispose)
        {
            _inner = inner;
            _connection = connection;
            _onDispose = onDispose;
        }

        protected override DbConnection? DbConnection => _connection;
        public override IsolationLevel IsolationLevel => _inner.IsolationLevel;
        public override void Commit() => _inner.Commit();
        public override void Rollback() => _inner.Rollback();
        public override Task CommitAsync(CancellationToken cancellationToken = default) => _inner.CommitAsync(cancellationToken);
        public override Task RollbackAsync(CancellationToken cancellationToken = default) => _inner.RollbackAsync(cancellationToken);

        protected override void Dispose(bool disposing)
        {
            _onDispose();
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            _onDispose();
            await _inner.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A pass-through DbCommand that unwraps TrackingTransaction back to the inner SqliteTransaction
    /// so that SQLite accepts it (it rejects foreign transaction types).
    /// </summary>
    private sealed class PassthroughCommand : DbCommand
    {
        private readonly SqliteCommand _inner;
        private readonly TrackingConnection _conn;

        public PassthroughCommand(SqliteCommand inner, TrackingConnection conn)
        {
            _inner = inner;
            _conn = conn;
        }

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get => _inner.CommandText; set => _inner.CommandText = value!; }
        public override int CommandTimeout { get => _inner.CommandTimeout; set => _inner.CommandTimeout = value; }
        public override CommandType CommandType { get => _inner.CommandType; set => _inner.CommandType = value; }
        public override bool DesignTimeVisible { get => false; set { } }
        public override UpdateRowSource UpdatedRowSource { get => _inner.UpdatedRowSource; set => _inner.UpdatedRowSource = value; }

        protected override DbConnection? DbConnection
        {
            get => _conn;
            set { /* always bound to _conn */ }
        }

        protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => _inner.Transaction;
            set
            {
                // Unwrap TrackingTransaction to get the real SqliteTransaction
                if (value is TrackingTransaction tt)
                {
                    var field = typeof(TrackingTransaction)
                        .GetField("_inner", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                    _inner.Transaction = (SqliteTransaction?)field?.GetValue(tt);
                }
                else
                {
                    _inner.Transaction = (SqliteTransaction?)value;
                }
            }
        }

        public override void Cancel() => _inner.Cancel();
        public override int ExecuteNonQuery() => _inner.ExecuteNonQuery();
        public override object? ExecuteScalar() => _inner.ExecuteScalar();
        public override void Prepare() => _inner.Prepare();
        protected override DbParameter CreateDbParameter() => _inner.CreateParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => _inner.ExecuteReader(behavior);

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
            => _inner.ExecuteNonQueryAsync(cancellationToken);

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
            => _inner.ExecuteScalarAsync(cancellationToken);

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior, CancellationToken cancellationToken)
        {
            DbDataReader r = await _inner.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
            return r;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    /// <summary>
    /// A SqliteProvider subclass that accepts TrackingConnection (not just SqliteConnection)
    /// so ValidateConnection doesn't reject the wrapper.
    /// </summary>
    private sealed class TrackingProvider : SqliteProvider
    {
        protected override void ValidateConnection(System.Data.Common.DbConnection connection)
        {
            // Accept both SqliteConnection and our TrackingConnection wrapper
            if (connection is not Microsoft.Data.Sqlite.SqliteConnection && connection is not TrackingConnection)
                throw new InvalidOperationException("Expected SqliteConnection or TrackingConnection.");
        }

        public override async Task InitializeConnectionAsync(System.Data.Common.DbConnection connection, System.Threading.CancellationToken ct)
        {
            // Run PRAGMA commands — TrackingConnection.CreateCommand() delegates to the inner SqliteCommand
            await using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            await pragmaCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        public override void InitializeConnection(System.Data.Common.DbConnection connection)
        {
            using var pragmaCmd = connection.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA synchronous = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -2000000; PRAGMA busy_timeout = 5000;";
            pragmaCmd.ExecuteNonQuery();
        }
    }

    private static (TrackingConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new TrackingConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new TrackingProvider());
        return (cn, ctx);
    }

    // ── CT-1/TX-1 Tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task InsertAsync_WithoutExternalTransaction_DisposesInternalTransaction()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        var item = new TxLifecycleItem { Name = "Test", Value = 42 };
        await ctx.InsertAsync(item);

        Assert.NotEmpty(cn.TransactionWasDisposed);
        Assert.All(cn.TransactionWasDisposed, wasDisposed =>
            Assert.True(wasDisposed,
                "CT-1/TX-1: Transaction created by InsertAsync must be disposed after completion."));
    }

    [Fact]
    public async Task UpdateAsync_WithoutExternalTransaction_DisposesInternalTransaction()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        var item = new TxLifecycleItem { Name = "Initial", Value = 1 };
        await ctx.InsertAsync(item);

        int countBefore = cn.TransactionWasDisposed.Count;

        item.Name = "Updated";
        await ctx.UpdateAsync(item);

        // Verify all transactions from UpdateAsync (and any since countBefore) were disposed
        for (int i = countBefore; i < cn.TransactionWasDisposed.Count; i++)
        {
            Assert.True(cn.TransactionWasDisposed[i],
                $"CT-1/TX-1: Transaction [{i}] created by UpdateAsync must be disposed after completion.");
        }
    }

    [Fact]
    public async Task DeleteAsync_WithoutExternalTransaction_DisposesInternalTransaction()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        var item = new TxLifecycleItem { Name = "ToDelete", Value = 99 };
        await ctx.InsertAsync(item);

        int countBefore = cn.TransactionWasDisposed.Count;

        await ctx.DeleteAsync(item);

        for (int i = countBefore; i < cn.TransactionWasDisposed.Count; i++)
        {
            Assert.True(cn.TransactionWasDisposed[i],
                $"CT-1/TX-1: Transaction [{i}] created by DeleteAsync must be disposed after completion.");
        }
    }

    [Fact]
    public async Task RepeatedInserts_NoResourceAccumulation_AllTransactionsDisposed()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        const int count = 12;
        for (int i = 0; i < count; i++)
        {
            await ctx.InsertAsync(new TxLifecycleItem { Name = $"Item{i}", Value = i });
        }

        // Every transaction that was created must have been disposed
        Assert.All(cn.TransactionWasDisposed, wasDisposed =>
            Assert.True(wasDisposed,
                "CT-1/TX-1: Every internal transaction must be disposed after each InsertAsync."));
    }

    [Fact]
    public async Task TransactionDisposal_EnsuredForAllOperations_InSequence()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        var item = new TxLifecycleItem { Name = "A", Value = 1 };
        await ctx.InsertAsync(item);

        int afterInsert = cn.TransactionWasDisposed.Count;
        Assert.True(afterInsert > 0);
        Assert.True(cn.TransactionWasDisposed[afterInsert - 1],
            "Transaction from InsertAsync must be disposed before UpdateAsync starts.");

        item.Name = "B";
        await ctx.UpdateAsync(item);

        int afterUpdate = cn.TransactionWasDisposed.Count;
        Assert.True(cn.TransactionWasDisposed[afterUpdate - 1],
            "Transaction from UpdateAsync must be disposed before DeleteAsync starts.");

        await ctx.DeleteAsync(item);

        int afterDelete = cn.TransactionWasDisposed.Count;
        Assert.True(cn.TransactionWasDisposed[afterDelete - 1],
            "Transaction from DeleteAsync must be disposed after completion.");
    }

    // ── TX-1: CurrentTransaction cleared after commit/rollback exception ─────

    /// <summary>
    /// TX-1: A transaction whose CommitAsync throws must still clear CurrentTransaction
    /// so that a subsequent BeginTransactionAsync call does not see "transaction already active".
    /// </summary>
    [Fact]
    public async Task AfterNormalCommit_CurrentTransaction_IsCleared()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
        await cmd.ExecuteNonQueryAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ctx = ctx;

        // Begin + commit normally
        var tx = await ctx.Database.BeginTransactionAsync();
        await tx.CommitAsync();

        // TX-1: CurrentTransaction must be null after normal commit
        Assert.Null(ctx.Database.CurrentTransaction);

        // And BeginTransactionAsync must succeed (no "already active" error)
        var tx2 = await ctx.Database.BeginTransactionAsync();
        await tx2.RollbackAsync();
        Assert.Null(ctx.Database.CurrentTransaction);
    }

    /// <summary>
    /// TX-1: A transaction whose RollbackAsync completes normally must still clear CurrentTransaction.
    /// </summary>
    [Fact]
    public async Task AfterNormalRollback_CurrentTransaction_IsCleared()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
        await cmd.ExecuteNonQueryAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ctx = ctx;

        var tx = await ctx.Database.BeginTransactionAsync();
        await tx.RollbackAsync();

        // TX-1: CurrentTransaction must be null after normal rollback
        Assert.Null(ctx.Database.CurrentTransaction);

        // And BeginTransactionAsync must succeed
        var tx2 = await ctx.Database.BeginTransactionAsync();
        await tx2.CommitAsync();
        Assert.Null(ctx.Database.CurrentTransaction);
    }

    /// <summary>
    /// TX-1: Tests that CommitAsync using try/finally properly clears CurrentTransaction even
    /// when the DbContextTransaction.CommitAsync path wraps the underlying commit in try/finally.
    /// Simulates the scenario by verifying multiple sequential transactions can be started.
    /// </summary>
    [Fact]
    public async Task AfterCommit_SequentialTransactions_AllSucceed()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
        await cmd.ExecuteNonQueryAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ctx = ctx;

        // TX-1: Multiple sequential commit+begin cycles must all succeed without "already active" error
        for (int i = 0; i < 5; i++)
        {
            var tx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(ctx.Database.CurrentTransaction);
            await tx.CommitAsync();
            Assert.Null(ctx.Database.CurrentTransaction);
        }
    }

    /// <summary>
    /// TX-1: Tests that RollbackAsync in try/finally correctly clears CurrentTransaction,
    /// allowing subsequent BeginTransactionAsync calls to succeed.
    /// </summary>
    [Fact]
    public async Task AfterRollback_SequentialTransactions_AllSucceed()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
        await cmd.ExecuteNonQueryAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ctx = ctx;

        // TX-1: Multiple sequential rollback+begin cycles must all succeed
        for (int i = 0; i < 5; i++)
        {
            var tx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(ctx.Database.CurrentTransaction);
            await tx.RollbackAsync();
            Assert.Null(ctx.Database.CurrentTransaction);
        }
    }

    /// <summary>
    /// TX-1: Verifies that CommitAsync exception path correctly clears CurrentTransaction.
    /// Uses a FailingTransaction wrapper that throws on CommitAsync.
    /// </summary>
    [Fact]
    public async Task AfterCommitException_CurrentTransaction_IsCleared()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ctx = ctx;

        // Manually inject a DbContextTransaction wrapping a FailingTransaction
        // to test the try/finally cleanup path on commit failure.
        var innerTx = await cn.BeginTransactionAsync();
        var failingTx = new FailingTransaction(innerTx);

        // Set CurrentTransaction manually via reflection (simulating what BeginTransactionAsync does)
        ctx.CurrentTransaction = failingTx;
        Assert.NotNull(ctx.Database.CurrentTransaction);

        var dbContextTx = new DbContextTransaction(failingTx, ctx);

        // CommitAsync should throw, but CurrentTransaction must be cleared in finally
        await Assert.ThrowsAsync<InvalidOperationException>(() => dbContextTx.CommitAsync());

        // TX-1: Even after CommitAsync threw, CurrentTransaction must be null
        Assert.Null(ctx.Database.CurrentTransaction);
    }

    /// <summary>
    /// TX-1: Verifies that RollbackAsync exception path correctly clears CurrentTransaction.
    /// </summary>
    [Fact]
    public async Task AfterRollbackException_CurrentTransaction_IsCleared()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ctx = ctx;

        var innerTx = await cn.BeginTransactionAsync();
        var failingTx = new FailingTransaction(innerTx);

        ctx.CurrentTransaction = failingTx;
        Assert.NotNull(ctx.Database.CurrentTransaction);

        var dbContextTx = new DbContextTransaction(failingTx, ctx);

        // RollbackAsync should throw, but CurrentTransaction must be cleared in finally
        await Assert.ThrowsAsync<InvalidOperationException>(() => dbContextTx.RollbackAsync());

        // TX-1: Even after RollbackAsync threw, CurrentTransaction must be null
        Assert.Null(ctx.Database.CurrentTransaction);
    }

    /// <summary>
    /// A DbTransaction wrapper that always throws InvalidOperationException on
    /// CommitAsync and RollbackAsync to simulate network or server errors.
    /// </summary>
    private sealed class FailingTransaction : DbTransaction
    {
        private readonly DbTransaction _inner;

        public FailingTransaction(DbTransaction inner)
        {
            _inner = inner;
        }

        protected override DbConnection DbConnection => _inner.Connection!;
        public override System.Data.IsolationLevel IsolationLevel => _inner.IsolationLevel;

        public override void Commit() => throw new InvalidOperationException("Simulated commit failure.");
        public override void Rollback() => throw new InvalidOperationException("Simulated rollback failure.");

        public override Task CommitAsync(CancellationToken cancellationToken = default)
            => Task.FromException(new InvalidOperationException("Simulated commit failure."));

        public override Task RollbackAsync(CancellationToken cancellationToken = default)
            => Task.FromException(new InvalidOperationException("Simulated rollback failure."));

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync()
        {
            _inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
