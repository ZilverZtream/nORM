using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

//<summary>
//Verifies that migration runners use CancellationToken.None for CommitAsync so that
//a caller cancellation arriving during the commit acknowledgment window does NOT produce
//an ambiguous "failed but maybe committed" state.
//
//Strategy: wrap the connection/transaction to capture the token actually passed to CommitAsync.
//We also include a passthrough-command wrapper (same pattern as TransactionLifecycleTests)
//to satisfy SQLite's internal cast of DbTransaction → SqliteTransaction.
//</summary>
public class MigrationCommitCancellationTests
{
 // ── Token-observing connection + transaction + command wrappers ───────────

    private sealed class TokenCapturingConnection : DbConnection
    {
        private readonly SqliteConnection _inner;
        public CancellationToken? CapturedCommitToken { get; private set; }

        public TokenCapturingConnection(string cs) => _inner = new SqliteConnection(cs);

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

        public override void ChangeDatabase(string db) => _inner.ChangeDatabase(db);
        public override void Close() => _inner.Close();
        public override void Open() => _inner.Open();
        public override Task OpenAsync(CancellationToken ct) => _inner.OpenAsync(ct);

        protected override DbTransaction BeginDbTransaction(IsolationLevel il)
        {
            var tx = _inner.BeginTransaction(il);
            return new CapturingTransaction(tx, this);
        }

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
            IsolationLevel il, CancellationToken ct)
        {
            var tx = (SqliteTransaction)await _inner.BeginTransactionAsync(il, ct);
            return new CapturingTransaction(tx, this);
        }

        protected override DbCommand CreateDbCommand()
            => new CapturingCommand(_inner.CreateCommand(), this);

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }

        internal void RecordCommitToken(CancellationToken ct) => CapturedCommitToken = ct;

 // ── CapturingTransaction ──────────────────────────────────────────────

        internal sealed class CapturingTransaction : DbTransaction
        {
            internal readonly SqliteTransaction Inner;
            private readonly TokenCapturingConnection _owner;

            public CapturingTransaction(SqliteTransaction inner, TokenCapturingConnection owner)
            {
                Inner = inner;
                _owner = owner;
            }

            protected override DbConnection DbConnection => _owner;
            public override IsolationLevel IsolationLevel => Inner.IsolationLevel;
            public override void Commit() => Inner.Commit();
            public override void Rollback() => Inner.Rollback();

            public override async Task CommitAsync(CancellationToken ct = default)
            {
                _owner.RecordCommitToken(ct);
 // Always delegate with None so the actual commit can't be aborted.
                await Inner.CommitAsync(CancellationToken.None);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing) Inner.Dispose();
                base.Dispose(disposing);
            }

            public override ValueTask DisposeAsync() => Inner.DisposeAsync();
        }

 // ── CapturingCommand — unwraps CapturingTransaction to SqliteTransaction ─

        private sealed class CapturingCommand : DbCommand
        {
            private readonly SqliteCommand _inner;
            private readonly TokenCapturingConnection _conn;

            public CapturingCommand(SqliteCommand inner, TokenCapturingConnection conn)
            {
                _inner = inner;
                _conn = conn;
            }

            [System.Diagnostics.CodeAnalysis.AllowNull]
            public override string CommandText
            {
                get => _inner.CommandText;
                set => _inner.CommandText = value!;
            }

            public override int CommandTimeout
            {
                get => _inner.CommandTimeout;
                set => _inner.CommandTimeout = value;
            }

            public override CommandType CommandType
            {
                get => _inner.CommandType;
                set => _inner.CommandType = value;
            }

            public override bool DesignTimeVisible { get => false; set { } }

            public override UpdateRowSource UpdatedRowSource
            {
                get => _inner.UpdatedRowSource;
                set => _inner.UpdatedRowSource = value;
            }

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
 // test: unwrap CapturingTransaction → inner SqliteTransaction so that
 // SqliteCommand accepts it (it rejects non-SqliteTransaction types).
                    _inner.Transaction = value is CapturingTransaction ct ? ct.Inner : (SqliteTransaction?)value;
                }
            }

            public override void Cancel() => _inner.Cancel();
            public override int ExecuteNonQuery() => _inner.ExecuteNonQuery();
            public override object? ExecuteScalar() => _inner.ExecuteScalar();
            public override void Prepare() => _inner.Prepare();
            protected override DbParameter CreateDbParameter() => _inner.CreateParameter();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior b) => _inner.ExecuteReader(b);

            public override Task<int> ExecuteNonQueryAsync(CancellationToken ct)
                => _inner.ExecuteNonQueryAsync(ct);

            public override Task<object?> ExecuteScalarAsync(CancellationToken ct)
                => _inner.ExecuteScalarAsync(ct);

            protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
                CommandBehavior b, CancellationToken ct)
                => await _inner.ExecuteReaderAsync(b, ct);

            protected override void Dispose(bool disposing)
            {
                if (disposing) _inner.Dispose();
                base.Dispose(disposing);
            }

            public override ValueTask DisposeAsync() => _inner.DisposeAsync();
        }
    }

 // ── Tests ─────────────────────────────────────────────────────────────────

 //<summary>
 //SqliteMigrationRunner.CommitAsync must be called with CancellationToken.None,
 //not the caller-supplied token. This prevents ambiguous history state when a caller
 //times out or cancels near the commit boundary.
 //</summary>
    [Fact]
    public async Task SqliteMigrationRunner_CommitAsync_UsesNoneToken()
    {
        var conn = new TokenCapturingConnection("Data Source=:memory:");
        conn.Open();

        var runner = new SqliteMigrationRunner(conn, typeof(MigrationHistoryFaultTests).Assembly);
        await runner.ApplyMigrationsAsync(CancellationToken.None);

 // The token captured at CommitAsync must be the non-cancelable sentinel.
        Assert.NotNull(conn.CapturedCommitToken);
        Assert.Equal(CancellationToken.None, conn.CapturedCommitToken!.Value);
    }

 //<summary>
 //Migrations apply cleanly with a non-canceled token and the commit token is None.
 //</summary>
    [Fact]
    public async Task SqliteMigrationRunner_ApplyMigrations_CompletesSuccessfully_AndUsesNoneForCommit()
    {
        var conn = new TokenCapturingConnection("Data Source=:memory:");
        conn.Open();

        var runner = new SqliteMigrationRunner(conn, typeof(MigrationHistoryFaultTests).Assembly);
        await runner.ApplyMigrationsAsync();

 // Migrations applied without error.
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);

 // Commit used the non-cancelable token.
        Assert.Equal(CancellationToken.None, conn.CapturedCommitToken!.Value);
    }
}
