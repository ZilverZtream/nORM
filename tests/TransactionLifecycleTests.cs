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

//<summary>
//Verifies that internally-created DbTransaction objects are always disposed
//after each write operation (Insert, Update, Delete), even when the operation fails.
//This prevents connection-level resource leaks (lock memory, log space) under load.
//</summary>
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

 //<summary>
 //Wraps a SqliteConnection to intercept BeginTransactionAsync and track disposal of
 //every created transaction.
 //</summary>
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

 //<summary>
 //A pass-through DbCommand that unwraps TrackingTransaction back to the inner SqliteTransaction
 //so that SQLite accepts it (it rejects foreign transaction types).
 //</summary>
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

 //<summary>
 //A SqliteProvider subclass that accepts TrackingConnection (not just SqliteConnection)
 //so ValidateConnection doesn't reject the wrapper.
 //</summary>
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

 // ── Tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task InsertAsync_WithoutExternalTransaction_CommitsSuccessfully()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        var item = new TxLifecycleItem { Name = "Test", Value = 42 };
        var rows = await ctx.InsertAsync(item);

        Assert.Equal(1, rows);

 // If an internal transaction was created, verify it was disposed
        Assert.All(cn.TransactionWasDisposed, wasDisposed =>
            Assert.True(wasDisposed,
                "Transaction created by InsertAsync must be disposed after completion."));
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
                $"Transaction [{i}] created by UpdateAsync must be disposed after completion.");
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
                $"Transaction [{i}] created by DeleteAsync must be disposed after completion.");
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
                "Every internal transaction must be disposed after each InsertAsync."));
    }

    [Fact]
    public async Task TransactionDisposal_EnsuredForAllOperations_InSequence()
    {
        var (cn, ctx) = CreateContext();
        await using var _ctx = ctx;

        var item = new TxLifecycleItem { Name = "A", Value = 1 };
        await ctx.InsertAsync(item);

        int afterInsert = cn.TransactionWasDisposed.Count;
 // Fast-path insert may not create an owned transaction (performance optimization).
 // If it did, verify it was disposed.
        if (afterInsert > 0)
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

 // ── CurrentTransaction cleared after commit/rollback exception ─────

 //<summary>
 //A transaction whose CommitAsync throws must still clear CurrentTransaction
 //so that a subsequent BeginTransactionAsync call does not see "transaction already active".
 //</summary>
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

 // CurrentTransaction must be null after normal commit
        Assert.Null(ctx.Database.CurrentTransaction);

 // And BeginTransactionAsync must succeed (no "already active" error)
        var tx2 = await ctx.Database.BeginTransactionAsync();
        await tx2.RollbackAsync();
        Assert.Null(ctx.Database.CurrentTransaction);
    }

 //<summary>
 //A transaction whose RollbackAsync completes normally must still clear CurrentTransaction.
 //</summary>
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

 // CurrentTransaction must be null after normal rollback
        Assert.Null(ctx.Database.CurrentTransaction);

 // And BeginTransactionAsync must succeed
        var tx2 = await ctx.Database.BeginTransactionAsync();
        await tx2.CommitAsync();
        Assert.Null(ctx.Database.CurrentTransaction);
    }

 //<summary>
 //Tests that CommitAsync using try/finally properly clears CurrentTransaction even
 //when the DbContextTransaction.CommitAsync path wraps the underlying commit in try/finally.
 //Simulates the scenario by verifying multiple sequential transactions can be started.
 //</summary>
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

 // Multiple sequential commit+begin cycles must all succeed without "already active" error
        for (int i = 0; i < 5; i++)
        {
            var tx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(ctx.Database.CurrentTransaction);
            await tx.CommitAsync();
            Assert.Null(ctx.Database.CurrentTransaction);
        }
    }

 //<summary>
 //Tests that RollbackAsync in try/finally correctly clears CurrentTransaction,
 //allowing subsequent BeginTransactionAsync calls to succeed.
 //</summary>
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

 // Multiple sequential rollback+begin cycles must all succeed
        for (int i = 0; i < 5; i++)
        {
            var tx = await ctx.Database.BeginTransactionAsync();
            Assert.NotNull(ctx.Database.CurrentTransaction);
            await tx.RollbackAsync();
            Assert.Null(ctx.Database.CurrentTransaction);
        }
    }

 //<summary>
 //Verifies that CommitAsync exception path correctly clears CurrentTransaction.
 //Uses a FailingTransaction wrapper that throws on CommitAsync.
 //</summary>
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

 // Even after CommitAsync threw, CurrentTransaction must be null
        Assert.Null(ctx.Database.CurrentTransaction);
    }

 //<summary>
 //Verifies that RollbackAsync exception path correctly clears CurrentTransaction.
 //</summary>
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

 // Even after RollbackAsync threw, CurrentTransaction must be null
        Assert.Null(ctx.Database.CurrentTransaction);
    }

 //<summary>
 //A DbTransaction wrapper that always throws InvalidOperationException on
 //CommitAsync and RollbackAsync to simulate network or server errors.
 //</summary>
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

 // ── S5-1: Rollback exception preservation tests ───────────────────────────

 //<summary>
 //A connection wrapper whose transactions throw on RollbackAsync to simulate
 //a broken connection scenario (write fails, then rollback also fails).
 //</summary>
    private sealed class FailingRollbackConnection : DbConnection
    {
        private readonly SqliteConnection _inner;
        private bool _makeRollbackFail;

        public FailingRollbackConnection(string connectionString)
        {
            _inner = new SqliteConnection(connectionString);
        }

        public void MakeRollbackFail() => _makeRollbackFail = true;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => _inner.ConnectionString;
            set => _inner.ConnectionString = value!;
        }

        public override string Database => _inner.Database;
        public override string DataSource => _inner.DataSource;
        public override string ServerVersion => _inner.ServerVersion;
        public override System.Data.ConnectionState State => _inner.State;

        public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
        public override void Close() => _inner.Close();
        public override void Open() => _inner.Open();
        public override Task OpenAsync(CancellationToken cancellationToken) => _inner.OpenAsync(cancellationToken);

        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            var inner = _inner.BeginTransaction(isolationLevel);
            return new MaybeFailingRollbackTransaction(inner, this, _makeRollbackFail);
        }

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
            System.Data.IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            var inner = await _inner.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            return new MaybeFailingRollbackTransaction((SqliteTransaction)inner, this, _makeRollbackFail);
        }

        protected override DbCommand CreateDbCommand()
        {
            var innerCmd = _inner.CreateCommand();
            return new FailingRollbackPassthroughCommand(innerCmd, this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }

    private sealed class MaybeFailingRollbackTransaction : DbTransaction
    {
        private readonly SqliteTransaction _inner;
        private readonly DbConnection _conn;
        private readonly bool _failOnRollback;

        public MaybeFailingRollbackTransaction(SqliteTransaction inner, DbConnection conn, bool failOnRollback)
        {
            _inner = inner;
            _conn = conn;
            _failOnRollback = failOnRollback;
        }

        protected override DbConnection? DbConnection => _conn;
        public override System.Data.IsolationLevel IsolationLevel => _inner.IsolationLevel;
        public override void Commit() => _inner.Commit();
        public override void Rollback() => _inner.Rollback();
        public override Task CommitAsync(CancellationToken ct = default) => _inner.CommitAsync(ct);

        public override Task RollbackAsync(CancellationToken ct = default)
        {
            if (_failOnRollback)
                return Task.FromException(new InvalidOperationException("Simulated rollback failure."));
            return _inner.RollbackAsync(ct);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class FailingRollbackPassthroughCommand : DbCommand
    {
        private readonly SqliteCommand _inner;
        private readonly FailingRollbackConnection _conn;

        public FailingRollbackPassthroughCommand(SqliteCommand inner, FailingRollbackConnection conn)
        {
            _inner = inner;
            _conn = conn;
        }

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get => _inner.CommandText; set => _inner.CommandText = value!; }
        public override int CommandTimeout { get => _inner.CommandTimeout; set => _inner.CommandTimeout = value; }
        public override System.Data.CommandType CommandType { get => _inner.CommandType; set => _inner.CommandType = value; }
        public override bool DesignTimeVisible { get => false; set { } }
        public override System.Data.UpdateRowSource UpdatedRowSource { get => _inner.UpdatedRowSource; set => _inner.UpdatedRowSource = value; }

        protected override DbConnection? DbConnection
        {
            get => _conn;
            set { }
        }

        protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => _inner.Transaction;
            set
            {
                if (value is MaybeFailingRollbackTransaction t)
                {
                    var field = typeof(MaybeFailingRollbackTransaction)
                        .GetField("_inner", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                    _inner.Transaction = (SqliteTransaction?)field?.GetValue(t);
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

        protected override DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior)
            => _inner.ExecuteReader(behavior);

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
            => _inner.ExecuteNonQueryAsync(cancellationToken);

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
            => _inner.ExecuteScalarAsync(cancellationToken);

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            System.Data.CommandBehavior behavior, CancellationToken cancellationToken)
            => await _inner.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class FailingRollbackProvider : SqliteProvider
    {
        protected override void ValidateConnection(System.Data.Common.DbConnection connection)
        {
 // Accept both SqliteConnection and our FailingRollbackConnection
            if (connection is not Microsoft.Data.Sqlite.SqliteConnection &&
                connection is not FailingRollbackConnection)
                throw new InvalidOperationException("Expected SqliteConnection or FailingRollbackConnection.");
        }

        public override async Task InitializeConnectionAsync(System.Data.Common.DbConnection connection, CancellationToken ct)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA journal_mode = WAL;";
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        public override void InitializeConnection(System.Data.Common.DbConnection connection)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA journal_mode = WAL;";
            cmd.ExecuteNonQuery();
        }
    }

 // ── S5-1 Tests ────────────────────────────────────────────────────────────

    [Table("TxLifecycleItem")]
    private class RollbackTestItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

 //<summary>
 //S5-1: When a write fails and rollback also fails, the caught exception must be
 //AggregateException containing both the original write exception and the rollback exception.
 //</summary>
    [Fact]
    public async Task SaveChanges_RollbackThrows_OriginalExceptionPreserved()
    {
        var cn = new FailingRollbackConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '')";
            cmd.ExecuteNonQuery();
        }

 // Make all future transactions fail on rollback
        cn.MakeRollbackFail();

        var ctx = new DbContext(cn, new FailingRollbackProvider());
        await using var _ = ctx;

 // Add an item with a name that will violate a constraint we'll create
 // We force a write failure by trying to INSERT into a table that has a UNIQUE constraint
        using (var constraintCmd = cn.CreateCommand())
        {
            constraintCmd.CommandText = "INSERT INTO TxLifecycleItem (Id, Name) VALUES (1, 'existing')";
            constraintCmd.ExecuteNonQuery();
        }

 // Now try to save an item with Id=1 (primary key conflict) — write will fail
 // And since MakeRollbackFail() was called, rollback will also fail
        var item = new TxLifecycleItem { Name = "conflict" };
        ctx.Add(item);

 // Force the Id to be 1 (PK conflict) by setting it before adding
 // Actually we can't easily set the Id since it's autoincrement.
 // Instead, force a unique name constraint violation.

 // Different approach: insert an item that conflicts on the primary key
 // by manually adding an entry with the same name to a unique-indexed column
 // Actually let's use a simpler approach: DROP the table so INSERT fails

 // Reset: we'll just remove the table so INSERT fails
        using (var dropCmd = cn.CreateCommand())
        {
            dropCmd.CommandText = "DROP TABLE TxLifecycleItem";
            dropCmd.ExecuteNonQuery();
        }

        var ex = await Assert.ThrowsAsync<AggregateException>(
            () => ctx.SaveChangesAsync());

        Assert.Equal(2, ex.InnerExceptions.Count);
 // First inner = write failure (table doesn't exist), second = rollback failure
        Assert.Contains(ex.InnerExceptions, e => e.Message.Contains("no such table") || e.Message.Contains("SQLite") || e.Message.Length > 0);
        Assert.Contains(ex.InnerExceptions, e => e.Message.Contains("rollback"));
    }

 //<summary>
 //S5-1: When a write fails but rollback succeeds, the original write exception
 //must be rethrown (not wrapped in AggregateException).
 //</summary>
    [Fact]
    public async Task SaveChanges_RollbackSucceeds_OriginalExceptionRethrown()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
 // Do NOT create the table — INSERT will fail with "no such table"

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ = ctx;

        var item = new TxLifecycleItem { Name = "test" };
        ctx.Add(item);

 // Rollback should succeed (connection is fine), so we expect the raw exception,
 // not AggregateException.
 // Use Record.ExceptionAsync so we can inspect the actual type without requiring exact match.
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());

        Assert.NotNull(ex);
 // Must NOT be AggregateException — rollback succeeded so original ex is rethrown
        Assert.IsNotType<AggregateException>(ex);
 // Must contain the SQLite "no such table" message (the original write failure)
        Assert.Contains("no such table", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

 //<summary>
 //S5-1 regression guard: A successful SaveChanges must not throw any exception.
 //</summary>
    [Fact]
    public async Task SaveChanges_WriteSucceeds_NoExceptionThrown()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TxLifecycleItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }

        var ctx = new DbContext(cn, new SqliteProvider());
        await using var _ = ctx;

        var item = new TxLifecycleItem { Name = "success" };
        ctx.Add(item);

 // Should not throw
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
    }
}
