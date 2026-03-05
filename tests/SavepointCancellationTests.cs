using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// PRV-1: Tests that savepoint operations correctly honour the CancellationToken.
/// Pre-cancelled tokens must throw OperationCanceledException rather than silently proceeding.
/// Also covers the normal savepoint flow (create → work → rollback → commit).
/// </summary>
public class SavepointCancellationTests
{
    private static async Task<SqliteConnection> CreateSchemaAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Sp_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();
        return cn;
    }

    // ─── SQLite: pre-cancelled token → CreateSavepointAsync throws ────────

    [Fact]
    public async Task SQLite_CreateSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new SqliteProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        await using var tx = await cn.BeginTransactionAsync();

        // PRV-1: Pre-cancelled token must cause OperationCanceledException
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp1", cts.Token));
    }

    [Fact]
    public async Task SQLite_RollbackToSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new SqliteProvider();

        await using var tx = await cn.BeginTransactionAsync();

        // Create the savepoint first with no cancellation
        await provider.CreateSavepointAsync(tx, "sp1");

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        // PRV-1: Pre-cancelled token must cause OperationCanceledException on rollback too
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1", cts.Token));
    }

    // ─── MySQL (via SqliteParameterFactory shim): pre-cancelled → throws ──

    [Fact]
    public async Task MySQL_CreateSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        // MySqlProvider uses reflection to find Save/CreateSavepoint/Savepoint methods.
        // We can test the CancellationToken check path independently of the actual MySQL driver
        // by using a pre-cancelled token — it throws before even looking for the save method.
        await using var cn = await CreateSchemaAsync();
        var provider = new MySqlProvider(new SqliteParameterFactory());

        await using var tx = await cn.BeginTransactionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        // PRV-1: Pre-cancelled token must cause OperationCanceledException
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp1", cts.Token));
    }

    [Fact]
    public async Task MySQL_RollbackToSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new MySqlProvider(new SqliteParameterFactory());

        await using var tx = await cn.BeginTransactionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1", cts.Token));
    }

    // ─── Normal flow: savepoint → work → rollback-to-savepoint ───────────

    [Fact]
    public async Task SQLite_Savepoint_NormalFlow_RollbackRemovesWork()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new SqliteProvider();
        using var cts = new CancellationTokenSource(); // not cancelled

        await using var tx = await cn.BeginTransactionAsync();

        // Insert before savepoint
        await using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO Sp_Item (Name) VALUES ('before')";
            await cmd.ExecuteNonQueryAsync();
        }

        await provider.CreateSavepointAsync(tx, "sp1", cts.Token);

        // Insert after savepoint
        await using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO Sp_Item (Name) VALUES ('after')";
            await cmd.ExecuteNonQueryAsync();
        }

        // Roll back to savepoint (removes 'after' insert)
        await provider.RollbackToSavepointAsync(tx, "sp1", cts.Token);
        await tx.CommitAsync();

        await using var countCmd = cn.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM Sp_Item";
        var count = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
        Assert.Equal(1L, count); // only 'before' survived
    }

    // ─── CancellationToken.None is always valid ───────────────────────────

    [Fact]
    public async Task SQLite_CreateSavepointAsync_NoneToken_DoesNotThrow()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new SqliteProvider();

        await using var tx = await cn.BeginTransactionAsync();

        // Should not throw
        await provider.CreateSavepointAsync(tx, "sp_none", CancellationToken.None);
    }

    [Fact]
    public async Task SQLite_RollbackToSavepointAsync_NoneToken_DoesNotThrow()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new SqliteProvider();

        await using var tx = await cn.BeginTransactionAsync();
        await provider.CreateSavepointAsync(tx, "sp_none");

        // Should not throw
        await provider.RollbackToSavepointAsync(tx, "sp_none", CancellationToken.None);
        await tx.CommitAsync();
    }

    // ─── SQL Server: pre-cancelled token → throws before SQL Server check ─

    /// <summary>
    /// PRV-1: SqlServerProvider.CreateSavepointAsync must honour a pre-cancelled token
    /// and throw OperationCanceledException BEFORE attempting the SqlTransaction check.
    /// We use a SQLite transaction here because the CancellationToken check fires first.
    /// </summary>
    [Fact]
    public async Task SqlServer_CreateSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        // SqlServerProvider now calls ct.ThrowIfCancellationRequested() as the very first
        // statement, before checking for SqlTransaction. So a pre-cancelled token throws
        // regardless of the transaction type.
        await using var cn = await CreateSchemaAsync();
        var provider = new SqlServerProvider();

        await using var tx = await cn.BeginTransactionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        // PRV-1: Pre-cancelled token must cause OperationCanceledException
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp1", cts.Token));
    }

    /// <summary>
    /// PRV-1: SqlServerProvider.RollbackToSavepointAsync must honour a pre-cancelled token.
    /// </summary>
    [Fact]
    public async Task SqlServer_RollbackToSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new SqlServerProvider();

        await using var tx = await cn.BeginTransactionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        // PRV-1: Pre-cancelled token must cause OperationCanceledException
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1", cts.Token));
    }

    // ─── Postgres: pre-cancelled token → throws before reflection check ───

    /// <summary>
    /// PRV-1: PostgresProvider.CreateSavepointAsync must honour a pre-cancelled token.
    /// </summary>
    [Fact]
    public async Task Postgres_CreateSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new PostgresProvider(new SqliteParameterFactory());

        await using var tx = await cn.BeginTransactionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        // PRV-1: Pre-cancelled token must cause OperationCanceledException
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp1", cts.Token));
    }

    /// <summary>
    /// PRV-1: PostgresProvider.RollbackToSavepointAsync must honour a pre-cancelled token.
    /// </summary>
    [Fact]
    public async Task Postgres_RollbackToSavepointAsync_PreCancelled_ThrowsOperationCanceled()
    {
        await using var cn = await CreateSchemaAsync();
        var provider = new PostgresProvider(new SqliteParameterFactory());

        await using var tx = await cn.BeginTransactionAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1", cts.Token));
    }
}
