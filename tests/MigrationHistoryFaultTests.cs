using System;
using System.Collections.Generic;
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

/// <summary>
/// Verifies that GetAppliedMigrationsAsync handles the "history table does not exist"
/// case silently while propagating all other DbException types to the caller.
/// </summary>
public class MigrationHistoryFaultTests
{
 // ── Minimal migration used across all tests ───────────────────────────────

    private class CreateItemTable : nORM.Migration.Migration
    {
 // Use a high version number to avoid conflict with SqliteMigrationRunnerTests (v1, v2).
        public CreateItemTable() : base(100, nameof(CreateItemTable)) { }

        public override void Up(DbConnection connection, DbTransaction transaction, CancellationToken ct = default)
        {
            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "CREATE TABLE MgFaultItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        public override void Down(DbConnection connection, DbTransaction transaction, CancellationToken ct = default)
        {
            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DROP TABLE MgFaultItem;";
            cmd.ExecuteNonQuery();
        }
    }

 // ── Fault-injection infrastructure ───────────────────────────────────────

 /// <summary>
 /// A concrete DbException whose message can be set explicitly in tests.
 /// </summary>
    private sealed class FaultDbException : DbException
    {
        public FaultDbException(string message) : base(message) { }
    }

 /// <summary>
 /// Wraps a SqliteConnection so that any command whose text contains <see cref="TriggerText"/>
 /// throws a <see cref="FaultDbException"/> instead of executing normally.
 ///
 /// Only commands that match the trigger are faulted; all other commands pass through to the
 /// real SQLite engine so that setup (EnsureHistoryTable, etc.) can run without interference.
 /// </summary>
    private sealed class FaultInjectingConnection : DbConnection
    {
        private readonly SqliteConnection _inner;
        private readonly string _triggerText;
        private readonly string _faultMessage;

 /// <summary>
 /// Number of times the fault was raised.
 /// </summary>
        public int FaultCount { get; private set; }

        public FaultInjectingConnection(string connectionString, string triggerText, string faultMessage)
        {
            _inner = new SqliteConnection(connectionString);
            _triggerText = triggerText;
            _faultMessage = faultMessage;
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
            => _inner.BeginTransaction(isolationLevel);

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
            IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            var tx = await _inner.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            return tx;
        }

        protected override DbCommand CreateDbCommand() => new FaultCommand(_inner.CreateCommand(), _triggerText, _faultMessage, this);

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

        internal void RecordFault() => FaultCount++;

 /// <summary>
 /// Pass-through DbCommand that throws FaultDbException when the SQL matches the trigger text.
 /// </summary>
        private sealed class FaultCommand : DbCommand
        {
            private readonly SqliteCommand _inner;
            private readonly string _triggerText;
            private readonly string _faultMessage;
            private readonly FaultInjectingConnection _owner;

            public FaultCommand(SqliteCommand inner, string triggerText, string faultMessage, FaultInjectingConnection owner)
            {
                _inner = inner;
                _triggerText = triggerText;
                _faultMessage = faultMessage;
                _owner = owner;
            }

            [System.Diagnostics.CodeAnalysis.AllowNull]
            public override string CommandText
            {
                get => _inner.CommandText;
                set => _inner.CommandText = value!;
            }

            public override int CommandTimeout { get => _inner.CommandTimeout; set => _inner.CommandTimeout = value; }
            public override CommandType CommandType { get => _inner.CommandType; set => _inner.CommandType = value; }
            public override bool DesignTimeVisible { get => false; set { } }
            public override UpdateRowSource UpdatedRowSource { get => _inner.UpdatedRowSource; set => _inner.UpdatedRowSource = value; }

            protected override DbConnection? DbConnection { get => _owner; set { } }
            protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

            protected override DbTransaction? DbTransaction
            {
                get => _inner.Transaction;
                set => _inner.Transaction = (SqliteTransaction?)value;
            }

            private bool ShouldFault => _inner.CommandText?.Contains(_triggerText, StringComparison.OrdinalIgnoreCase) == true;

            public override void Cancel() => _inner.Cancel();
            public override void Prepare() => _inner.Prepare();
            protected override DbParameter CreateDbParameter() => _inner.CreateParameter();

            public override int ExecuteNonQuery()
            {
                if (ShouldFault) { _owner.RecordFault(); throw new FaultDbException(_faultMessage); }
                return _inner.ExecuteNonQuery();
            }

            public override object? ExecuteScalar()
            {
                if (ShouldFault) { _owner.RecordFault(); throw new FaultDbException(_faultMessage); }
                return _inner.ExecuteScalar();
            }

            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            {
                if (ShouldFault) { _owner.RecordFault(); throw new FaultDbException(_faultMessage); }
                return _inner.ExecuteReader(behavior);
            }

            public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
            {
                if (ShouldFault) { _owner.RecordFault(); throw new FaultDbException(_faultMessage); }
                return _inner.ExecuteNonQueryAsync(cancellationToken);
            }

            public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
            {
                if (ShouldFault) { _owner.RecordFault(); throw new FaultDbException(_faultMessage); }
                return _inner.ExecuteScalarAsync(cancellationToken);
            }

            protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
                CommandBehavior behavior, CancellationToken cancellationToken)
            {
                if (ShouldFault) { _owner.RecordFault(); throw new FaultDbException(_faultMessage); }
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
    }

 // ── Tests ───────────────────────────────────────────────────────────

 /// <summary>
 /// When the migrations history table does not yet exist, GetAppliedMigrationsAsync
 /// must return an empty set rather than throwing. This is the expected first-run state.
 /// </summary>
    [Fact]
    public async Task GetAppliedMigrations_WhenHistoryTableMissing_ReturnsEmpty()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

 // Use a test-only assembly that has NO migrations so HasPendingMigrations
 // calls GetAppliedMigrations with no history table present.
        var runner = new SqliteMigrationRunner(connection, typeof(MigrationHistoryFaultTests).Assembly);

 // Should not throw even though the history table doesn't exist yet.
        var pending = await runner.GetPendingMigrationsAsync();

 // The test assembly contains this migration class at version 100.
        Assert.Contains("100_CreateItemTable", pending);
    }

 /// <summary>
 /// When a transient DbException occurs (e.g., permission denied, connection reset)
 /// during GetAppliedMigrationsAsync, the exception must NOT be swallowed — it must propagate
 /// to the caller so the application can react (retry, alert, etc.).
 /// </summary>
    [Fact]
    public async Task GetAppliedMigrations_WhenTransientDbException_Propagates()
    {
 // Use a FaultInjectingConnection that throws a non-"table not found" error
 // when the history SELECT is executed. The trigger text matches the history table name.
        const string faultMessage = "disk I/O error";
        var conn = new FaultInjectingConnection(
            "Data Source=:memory:",
            triggerText: "__NormMigrationsHistory",
            faultMessage: faultMessage);
        conn.Open();

 // Pre-create the history table so EnsureHistoryTableAsync doesn't interfere,
 // but the fault-injecting connection will throw when SELECT is attempted.
 // Actually: EnsureHistoryTable uses CREATE TABLE IF NOT EXISTS — that also matches the trigger.
 // We need to create the table with the direct inner connection to bypass the fault.
 // Access the inner via reflection (only needed in this controlled test).
        var innerField = typeof(FaultInjectingConnection)
            .GetField("_inner", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var innerConn = (SqliteConnection)innerField.GetValue(conn)!;
        using (var setupCmd = innerConn.CreateCommand())
        {
            setupCmd.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" (Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
            setupCmd.ExecuteNonQuery();
        }

        var runner = new SqliteMigrationRunner(conn, typeof(MigrationHistoryFaultTests).Assembly);

 // GetPendingMigrationsAsync → GetAppliedMigrationsAsync → SELECT from history table → fault
        var ex = await Assert.ThrowsAsync<FaultDbException>(
            () => runner.GetPendingMigrationsAsync());

        Assert.Contains(faultMessage, ex.Message, StringComparison.Ordinal);
        Assert.True(conn.FaultCount >= 1,
            "The fault-injecting connection should have been called at least once.");
    }

 /// <summary>
 /// After the history table has been created and a migration recorded,
 /// GetAppliedMigrationsAsync must accurately return the applied set without error.
 /// </summary>
    [Fact]
    public async Task GetAppliedMigrations_AfterHistoryTableCreated_ReturnsAppliedMigrations()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var runner = new SqliteMigrationRunner(connection, typeof(MigrationHistoryFaultTests).Assembly);

 // Apply migrations (creates history table and records entries).
        await runner.ApplyMigrationsAsync();

 // Now no migrations should be pending.
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);

 // Confirm the history table records our migration.
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = 100;";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(1L, count);
    }

 /// <summary>
 /// HasPendingMigrationsAsync must also propagate transient DbExceptions from
 /// GetAppliedMigrationsAsync rather than masking them as "no pending migrations".
 /// </summary>
    [Fact]
    public async Task HasPendingMigrations_WhenTransientDbException_Propagates()
    {
        const string faultMessage = "connection forcibly closed";
        var conn = new FaultInjectingConnection(
            "Data Source=:memory:",
            triggerText: "__NormMigrationsHistory",
            faultMessage: faultMessage);
        conn.Open();

 // Create the history table on the inner connection so EnsureHistoryTable won't throw,
 // but the SELECT will still be faulted by FaultCommand.
        var innerField = typeof(FaultInjectingConnection)
            .GetField("_inner", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var innerConn = (SqliteConnection)innerField.GetValue(conn)!;
        using (var setupCmd = innerConn.CreateCommand())
        {
            setupCmd.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" (Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
            setupCmd.ExecuteNonQuery();
        }

        var runner = new SqliteMigrationRunner(conn, typeof(MigrationHistoryFaultTests).Assembly);

        var ex = await Assert.ThrowsAsync<FaultDbException>(
            () => runner.HasPendingMigrationsAsync());

        Assert.Contains(faultMessage, ex.Message, StringComparison.Ordinal);
    }
}
