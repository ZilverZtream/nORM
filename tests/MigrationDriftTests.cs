using System;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>MIG-1: Verify migration runner throws when a version's name drifts.</summary>
public class MigrationDriftTests
{
    [Fact]
    public async Task GetPendingMigrations_Throws_WhenNameDrifts()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Seed history: version=1, but with OLD name that doesn't match "CreateBlogTable"
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(\"Version\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"AppliedOn\" TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd = cn.CreateCommand())
        {
            // Version 1 is applied but with an old name (before rename)
            cmd.CommandText = "INSERT INTO \"__NormMigrationsHistory\" VALUES (1, 'OldBlogMigration', '2024-01-01')";
            await cmd.ExecuteNonQueryAsync();
        }

        // Use SqliteMigrationRunnerTests's assembly which contains CreateBlogTable (v1, "CreateBlogTable")
        var runner = new SqliteMigrationRunner(cn, typeof(SqliteMigrationRunnerTests).Assembly);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.GetPendingMigrationsAsync());

        Assert.Contains("name drift", ex.Message);
        Assert.Contains("OldBlogMigration", ex.Message);
        Assert.Contains("CreateBlogTable", ex.Message);
    }

    [Fact]
    public async Task GetPendingMigrations_NoThrow_WhenNameMatches()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Seed history: version=1, name="CreateBlogTable" — matches exactly
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(\"Version\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"AppliedOn\" TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO \"__NormMigrationsHistory\" VALUES (1, 'CreateBlogTable', '2024-01-01')";
            await cmd.ExecuteNonQueryAsync();
        }

        var runner = new SqliteMigrationRunner(cn, typeof(SqliteMigrationRunnerTests).Assembly);

        // No exception — name matches for v1
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.NotNull(pending);
    }
}
