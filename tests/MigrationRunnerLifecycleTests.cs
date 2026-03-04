using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// MIG-6: Verifies that migration runners implement IAsyncDisposable/IDisposable
/// and that disposal completes without exception and releases the internal context.
/// </summary>
public class MigrationRunnerLifecycleTests
{
    private static Assembly MigrationsAssembly => typeof(SqliteMigrationRunnerTests).Assembly;

    [Fact]
    public async Task SqliteMigrationRunner_DisposeAsync_DoesNotThrow()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var options = new DbContextOptions();
        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);

        // Should not throw
        await runner.DisposeAsync();
    }

    [Fact]
    public async Task SqliteMigrationRunner_Dispose_DoesNotThrow()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var options = new DbContextOptions();
        using var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);
        // Dispose is called via using - should not throw
    }

    [Fact]
    public async Task SqliteMigrationRunner_DoubleDispose_DoesNotThrow()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var options = new DbContextOptions();
        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly, options);

        await runner.DisposeAsync();
        // Second dispose should be a no-op
        await runner.DisposeAsync();
    }

    [Fact]
    public async Task SqliteMigrationRunner_WithoutOptions_DisposeAsync_DoesNotThrow()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly);

        // Should not throw even without options (no internal _context created)
        await runner.DisposeAsync();
    }

    [Fact]
    public async Task SqliteMigrationRunner_IsIAsyncDisposable()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var runner = new SqliteMigrationRunner(cn, MigrationsAssembly);

        // Verify that runner implements IAsyncDisposable
        Assert.IsAssignableFrom<IAsyncDisposable>(runner);
        Assert.IsAssignableFrom<IDisposable>(runner);

        await runner.DisposeAsync();
    }
}
