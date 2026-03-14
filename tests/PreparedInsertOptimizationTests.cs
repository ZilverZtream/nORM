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

namespace nORM.Tests;

public class PreparedInsertOptimizationTests
{
    [Table("PreparedInsertItem")]
    private sealed class PreparedInsertItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private static void CreateTable(DbConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE PreparedInsertItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(DbConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM PreparedInsertItem";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task InsertAsync_reuses_cached_command_and_avoids_owned_transactions()
    {
        await using var connection = new TrackingConnection("Data Source=:memory:");
        connection.Open();
        CreateTable(connection);
        connection.ResetCounters();

        await using var context = new DbContext(connection, new TrackingSqliteProvider());

        var first = new PreparedInsertItem { Name = "first" };
        var second = new PreparedInsertItem { Name = "second" };

        await context.InsertAsync(first);
        await context.InsertAsync(second);

        Assert.Equal(0, connection.BeginTransactionCallCount);
        // One command is spent on provider initialization; the prepared insert command is reused.
        Assert.Equal(2, connection.CreateCommandCallCount);
        Assert.Equal(1, connection.PrepareCallCount);
        Assert.True(first.Id > 0);
        Assert.True(second.Id > 0);
        Assert.Equal(2L, CountRows(connection));
    }

    [Fact]
    public async Task InsertAsync_recreates_cached_command_when_transaction_binding_changes()
    {
        await using var connection = new TrackingConnection("Data Source=:memory:");
        connection.Open();
        CreateTable(connection);
        connection.ResetCounters();

        await using var context = new DbContext(connection, new TrackingSqliteProvider());

        await context.InsertAsync(new PreparedInsertItem { Name = "autocommit-1" });

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            await context.InsertAsync(new PreparedInsertItem { Name = "explicit-tx" });
            await transaction.CommitAsync();
        }

        await context.InsertAsync(new PreparedInsertItem { Name = "autocommit-2" });

        Assert.Equal(1, connection.BeginTransactionCallCount);
        // Provider initialization adds one command on top of the three transaction-bound insert shapes.
        Assert.Equal(4, connection.CreateCommandCallCount);
        Assert.Equal(3, connection.PrepareCallCount);
        Assert.Equal(3L, CountRows(connection));
    }

    [Fact]
    public async Task PrepareInsertAsync_default_hydrates_generated_keys()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        CreateTable(connection);

        var interceptor = new CommandCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        await using var context = new DbContext(connection, new TrackingSqliteProvider(), options);
        await using var prepared = await context.PrepareInsertAsync<PreparedInsertItem>();

        var entity = new PreparedInsertItem { Name = "hydrated" };
        var affected = await prepared.ExecuteAsync(entity);

        Assert.Equal(1, affected);
        Assert.True(entity.Id > 0);
        Assert.Equal(1, interceptor.ScalarExecutions);
        Assert.Equal(0, interceptor.NonQueryExecutions);
        // SQLite uses RETURNING clause for identity retrieval (or last_insert_rowid fallback)
        Assert.True(
            interceptor.LastCommandText.Contains("RETURNING", StringComparison.OrdinalIgnoreCase) ||
            interceptor.LastCommandText.Contains("last_insert_rowid", StringComparison.OrdinalIgnoreCase),
            $"Expected identity retrieval SQL, got: {interceptor.LastCommandText}");
    }

    [Fact]
    public async Task PrepareInsertAsync_can_skip_key_hydration_for_throughput()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        CreateTable(connection);

        var interceptor = new CommandCaptureInterceptor();
        var options = new DbContextOptions();
        options.CommandInterceptors.Add(interceptor);

        await using var context = new DbContext(connection, new TrackingSqliteProvider(), options);
        await using var prepared = await context.PrepareInsertAsync<PreparedInsertItem>(hydrateGeneratedKeys: false);

        var entity = new PreparedInsertItem { Name = "plain" };
        var affected = await prepared.ExecuteAsync(entity);

        Assert.Equal(1, affected);
        Assert.Equal(0, entity.Id);
        Assert.Equal(0, interceptor.ScalarExecutions);
        Assert.Equal(1, interceptor.NonQueryExecutions);
        Assert.DoesNotContain("RETURNING", interceptor.LastCommandText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("last_insert_rowid", interceptor.LastCommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1L, CountRows(connection));
    }

    private sealed class CommandCaptureInterceptor : IDbCommandInterceptor
    {
        public string LastCommandText { get; private set; } = string.Empty;
        public int ScalarExecutions { get; private set; }
        public int NonQueryExecutions { get; private set; }

        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            LastCommandText = command.CommandText;
            NonQueryExecutions++;
            return Task.FromResult(InterceptionResult<int>.Continue());
        }

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        {
            LastCommandText = command.CommandText;
            ScalarExecutions++;
            return Task.FromResult(InterceptionResult<object?>.Continue());
        }

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken ct)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken ct)
            => Task.CompletedTask;
    }

    private sealed class TrackingSqliteProvider : SqliteProvider
    {
        protected override void ValidateConnection(DbConnection connection)
        {
            if (connection is not SqliteConnection && connection is not TrackingConnection)
                throw new InvalidOperationException("Expected SqliteConnection or TrackingConnection.");
        }
    }

    private sealed class TrackingConnection : DbConnection
    {
        private readonly SqliteConnection _inner;

        public TrackingConnection(string connectionString)
        {
            _inner = new SqliteConnection(connectionString);
        }

        public int BeginTransactionCallCount { get; private set; }
        public int CreateCommandCallCount { get; private set; }
        public int PrepareCallCount { get; private set; }

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

        public void ResetCounters()
        {
            BeginTransactionCallCount = 0;
            CreateCommandCallCount = 0;
            PrepareCallCount = 0;
        }

        internal void RecordPrepare() => PrepareCallCount++;

        public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
        public override void Close() => _inner.Close();
        public override void Open() => _inner.Open();
        public override Task OpenAsync(CancellationToken cancellationToken) => _inner.OpenAsync(cancellationToken);

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            BeginTransactionCallCount++;
            return new TrackingTransaction(_inner.BeginTransaction(isolationLevel), this);
        }

        protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
            IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            BeginTransactionCallCount++;
            var inner = await _inner.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
            return new TrackingTransaction((SqliteTransaction)inner, this);
        }

        protected override DbCommand CreateDbCommand()
        {
            CreateCommandCallCount++;
            return new TrackingCommand(_inner.CreateCommand(), this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
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
        private readonly DbConnection _connection;

        public TrackingTransaction(SqliteTransaction inner, DbConnection connection)
        {
            _inner = inner;
            _connection = connection;
        }

        internal SqliteTransaction InnerTransaction => _inner;

        protected override DbConnection DbConnection => _connection;
        public override IsolationLevel IsolationLevel => _inner.IsolationLevel;
        public override void Commit() => _inner.Commit();
        public override void Rollback() => _inner.Rollback();
        public override Task CommitAsync(CancellationToken cancellationToken = default) => _inner.CommitAsync(cancellationToken);
        public override Task RollbackAsync(CancellationToken cancellationToken = default) => _inner.RollbackAsync(cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class TrackingCommand : DbCommand
    {
        private readonly SqliteCommand _inner;
        private readonly TrackingConnection _connection;

        public TrackingCommand(SqliteCommand inner, TrackingConnection connection)
        {
            _inner = inner;
            _connection = connection;
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

        public override bool DesignTimeVisible
        {
            get => false;
            set { }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => _inner.UpdatedRowSource;
            set => _inner.UpdatedRowSource = value;
        }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set { }
        }

        protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => _inner.Transaction;
            set => _inner.Transaction = value switch
            {
                null => null,
                TrackingTransaction trackingTransaction => trackingTransaction.InnerTransaction,
                SqliteTransaction sqliteTransaction => sqliteTransaction,
                _ => throw new InvalidOperationException("Expected a SqliteTransaction-compatible transaction.")
            };
        }

        public override void Cancel() => _inner.Cancel();
        public override int ExecuteNonQuery() => _inner.ExecuteNonQuery();
        public override object? ExecuteScalar() => _inner.ExecuteScalar();

        public override void Prepare()
        {
            _connection.RecordPrepare();
            _inner.Prepare();
        }

        public override Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            _connection.RecordPrepare();
            return _inner.PrepareAsync(cancellationToken);
        }

        protected override DbParameter CreateDbParameter() => _inner.CreateParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => _inner.ExecuteReader(behavior);

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
            => _inner.ExecuteNonQueryAsync(cancellationToken);

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
            => _inner.ExecuteScalarAsync(cancellationToken);

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
        {
            return await _inner.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
