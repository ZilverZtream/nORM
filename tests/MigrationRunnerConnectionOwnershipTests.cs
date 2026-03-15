using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Migration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//Verifies that migration runners do NOT dispose caller-owned connections
//when they are themselves disposed. The fix adds an _ownsConnection flag to DbContext so
//that contexts created over externally-supplied connections do not close them on disposal.
//</summary>
public class MigrationRunnerConnectionOwnershipTests
{
    private static Assembly MigrationsAssembly => typeof(SqliteMigrationRunnerTests).Assembly;

 // ─── Runner with interceptors: connection must survive runner disposal ─────

    [Fact]
    public async Task SqliteMigrationRunner_WithInterceptors_DisposeDoesNotCloseCallerConnection()
    {
 // When a runner is created with CommandInterceptors, it creates an internal
 // DbContext. Disposing the runner must not dispose the caller-supplied connection.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        Assert.Equal(ConnectionState.Open, cn.State);

        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new NoOpCommandInterceptor());

        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);
        await runner.DisposeAsync();

 // Connection must still be open and usable after the runner is disposed.
        Assert.Equal(ConnectionState.Open, cn.State);

 // Verify we can still execute queries on the connection.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var result = cmd.ExecuteScalar();
        Assert.Equal(1L, result);
    }

    [Fact]
    public async Task SqliteMigrationRunner_WithInterceptors_SyncDisposeDoesNotCloseCallerConnection()
    {
 // Same test for the synchronous Dispose path.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        Assert.Equal(ConnectionState.Open, cn.State);

        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new NoOpCommandInterceptor());

        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);
        runner.Dispose(); // synchronous dispose

        Assert.Equal(ConnectionState.Open, cn.State);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 42";
        var result = cmd.ExecuteScalar();
        Assert.Equal(42L, result);
    }

 // ─── Runner without interceptors: connection must also survive ────────────

    [Fact]
    public async Task SqliteMigrationRunner_WithoutInterceptors_DisposeDoesNotCloseCallerConnection()
    {
 // Even without interceptors (no internal DbContext), the runner must not
 // close the caller-supplied connection.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly);
        await runner.DisposeAsync();

        Assert.Equal(ConnectionState.Open, cn.State);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 99";
        var result = cmd.ExecuteScalar();
        Assert.Equal(99L, result);
    }

 // ─── Double-dispose of the runner is safe ─────────────────────────────────

    [Fact]
    public async Task SqliteMigrationRunner_DoubleDispose_WithInterceptors_DoesNotThrow()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new NoOpCommandInterceptor());

        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);
        await runner.DisposeAsync();
 // Second dispose must be a no-op.
        var ex = await Record.ExceptionAsync(() => runner.DisposeAsync().AsTask());
        Assert.Null(ex);

 // Connection must still be open.
        Assert.Equal(ConnectionState.Open, cn.State);
    }

 // ─── DbContext with external connection does not dispose it ───────────────

    [Fact]
    public async Task DbContext_WithExternalConnection_DisposeDoesNotCloseConnection()
    {
 // Verify the internal DbContext(cn, provider, options, ownsConnection:false)
 // constructor does not close the connection when the context is disposed.
 // We test this via the migration runner since the constructor is internal.
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var options = new DbContextOptions();
        options.CommandInterceptors.Add(new NoOpCommandInterceptor());

 // Create and immediately dispose the runner (which creates an internal DbContext).
        {
            await using var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);
 // runner.DisposeAsync() is called here via 'await using'
        }

 // The connection must still be open.
        Assert.Equal(ConnectionState.Open, cn.State);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT 7";
        Assert.Equal(7L, cmd.ExecuteScalar());
    }

 // ─── A separately-created, fully-owned DbContext DOES dispose its connection

    [Fact]
    public async Task DbContext_OwnsConnection_DisposeClosesConnection()
    {
 // Regression: The normal (caller-owns-the-context) path must still dispose the connection.
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var ctx = new DbContext(cn, new SqliteProvider());
        Assert.Equal(ConnectionState.Open, cn.State);

        await ctx.DisposeAsync();

 // The connection's State after dispose might be Closed or the object disposed —
 // either way it is no longer usable. We verify disposal happened by checking State.
        Assert.NotEqual(ConnectionState.Open, cn.State);
    }

 // ─── Helper interceptor ───────────────────────────────────────────────────

    private sealed class NoOpCommandInterceptor : IDbCommandInterceptor
    {
        public Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<int>.Continue());

        public Task NonQueryExecutedAsync(DbCommand command, DbContext context, int result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<object?>.Continue());

        public Task ScalarExecutedAsync(DbCommand command, DbContext context, object? result, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken cancellationToken)
            => Task.FromResult(InterceptionResult<DbDataReader>.Continue());

        public Task ReaderExecutedAsync(DbCommand command, DbContext context, DbDataReader reader, TimeSpan duration, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task CommandFailedAsync(DbCommand command, DbContext context, Exception exception, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
