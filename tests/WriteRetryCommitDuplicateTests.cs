using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Threading;
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
/// A single active-record write (InsertAsync) executed under a retry policy must not be replayed
/// once its commit has been attempted: a commit whose outcome is unknown could already have
/// persisted the row, so retrying it duplicates the write. The write path signals commit-attempted
/// and the retry strategy honours it, matching the guard on the SaveChanges retry path.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class WriteRetryCommitDuplicateTests
{
    [Table("WrWidget")]
    private class WrWidget
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static int RowCount(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM WrWidget";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    // Accepts the wrapper DbConnection (which is not itself a SqliteConnection).
    private sealed class PermissiveSqliteProvider : SqliteProvider
    {
        protected override void ValidateConnection(DbConnection connection) { }
    }

    [Fact]
    public async Task Insert_commit_failure_is_not_retried_so_row_is_not_duplicated()
    {
        // The wrapper's first CommitAsync really commits the row and THEN throws a retryable
        // error - the classic "commit succeeded but the acknowledgement was lost" case. Its
        // RollbackAsync is a no-op once committed, so the original (retryable) DbException reaches
        // the retry strategy. Without the commit-attempted guard the strategy replays the whole
        // insert and a second row is committed (duplicate). With the guard the error surfaces and
        // exactly one row remains.
        using var probe = new SqliteConnection("Data Source=:memory:");
        probe.Open();
        using (var cmd = probe.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE WrWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        await using var cn = new FailFirstCommitConnection(probe);
        var opts = new DbContextOptions
        {
            RetryPolicy = new RetryPolicy
            {
                MaxRetries = 3,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = ex => ex is SqliteException
            }
        };
        using var ctx = new DbContext(cn, new PermissiveSqliteProvider(), opts);

        Exception? captured = null;
        try
        {
            await ctx.InsertAsync(new WrWidget { Name = "only-once" });
        }
        catch (Exception ex)
        {
            captured = ex;
            // Expected once the guard is in place: the commit-time failure surfaces without retry.
        }

        Assert.True(cn.CommitCalls >= 1, $"commit never ran; captured={captured}");
        Assert.Equal(1, RowCount(probe));     // 2 before the fix: the insert was replayed and duplicated
    }

    // ── Connection/transaction/command decorator over SqliteConnection ───────────────────────────
    // Delegates everything to the inner SQLite objects, but the first CommitAsync commits and then
    // throws a retryable SqliteException; RollbackAsync becomes a no-op afterwards.
    private sealed class FailFirstCommitConnection : DbConnection
    {
        private readonly SqliteConnection _inner;
        public int CommitCalls { get; private set; }
        private bool _committed;

        public FailFirstCommitConnection(SqliteConnection inner) => _inner = inner;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _inner.ConnectionString; set => _inner.ConnectionString = value!; }
        public override string Database => _inner.Database;
        public override string DataSource => _inner.DataSource;
        public override string ServerVersion => _inner.ServerVersion;
        public override ConnectionState State => _inner.State;
        public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
        public override void Close() => _inner.Close();
        public override void Open() => _inner.Open();
        public override Task OpenAsync(CancellationToken ct) => _inner.OpenAsync(ct);

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => new FailFirstCommitTransaction((SqliteTransaction)_inner.BeginTransaction(isolationLevel), this);

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken ct)
        {
            var tx = await _inner.BeginTransactionAsync(isolationLevel, ct).ConfigureAwait(false);
            return new FailFirstCommitTransaction((SqliteTransaction)tx, this);
        }

        protected override DbCommand CreateDbCommand() => new PassthroughCommand(_inner.CreateCommand(), this);

        internal async Task CommitAsyncInner(SqliteTransaction inner, CancellationToken ct)
        {
            CommitCalls++;
            if (!_committed)
            {
                await inner.CommitAsync(ct).ConfigureAwait(false); // the row IS persisted...
                _committed = true;
                throw new SqliteException("Injected commit-time failure after commit persisted", 5 /* SQLITE_BUSY */);
            }
            await inner.CommitAsync(ct).ConfigureAwait(false);
        }

        internal Task RollbackAsyncInner(SqliteTransaction inner, CancellationToken ct)
            // Already committed above -> rolling back the inner tx would throw; treat as a no-op so
            // the original (retryable) commit exception is what reaches the retry strategy.
            => _committed ? Task.CompletedTask : inner.RollbackAsync(ct);

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

    private sealed class FailFirstCommitTransaction : DbTransaction
    {
        internal readonly SqliteTransaction Inner;
        private readonly FailFirstCommitConnection _conn;

        public FailFirstCommitTransaction(SqliteTransaction inner, FailFirstCommitConnection conn)
        {
            Inner = inner;
            _conn = conn;
        }

        protected override DbConnection? DbConnection => _conn;
        public override IsolationLevel IsolationLevel => Inner.IsolationLevel;
        public override void Commit() => _conn.CommitAsyncInner(Inner, CancellationToken.None).GetAwaiter().GetResult();
        public override void Rollback() => _conn.RollbackAsyncInner(Inner, CancellationToken.None).GetAwaiter().GetResult();
        public override Task CommitAsync(CancellationToken ct) => _conn.CommitAsyncInner(Inner, ct);
        public override Task RollbackAsync(CancellationToken ct) => _conn.RollbackAsyncInner(Inner, ct);

        protected override void Dispose(bool disposing)
        {
            if (disposing) Inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await Inner.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed class PassthroughCommand : DbCommand
    {
        private readonly SqliteCommand _inner;
        private readonly FailFirstCommitConnection _conn;

        public PassthroughCommand(SqliteCommand inner, FailFirstCommitConnection conn)
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
        protected override DbConnection? DbConnection { get => _conn; set { /* bound to _conn */ } }
        protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => _inner.Transaction;
            set => _inner.Transaction = value is FailFirstCommitTransaction ft ? ft.Inner : (SqliteTransaction?)value;
        }

        public override void Cancel() => _inner.Cancel();
        public override int ExecuteNonQuery() => _inner.ExecuteNonQuery();
        public override object? ExecuteScalar() => _inner.ExecuteScalar();
        public override void Prepare() => _inner.Prepare();
        protected override DbParameter CreateDbParameter() => _inner.CreateParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => _inner.ExecuteReader(behavior);
        public override Task<int> ExecuteNonQueryAsync(CancellationToken ct) => _inner.ExecuteNonQueryAsync(ct);
        public override Task<object?> ExecuteScalarAsync(CancellationToken ct) => _inner.ExecuteScalarAsync(ct);
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken ct)
            => await _inner.ExecuteReaderAsync(behavior, ct).ConfigureAwait(false);

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
