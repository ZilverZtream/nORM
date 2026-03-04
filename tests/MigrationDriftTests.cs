using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>MIG-1: Verify migration runner logs a warning when a version's name drifts.</summary>
public class MigrationDriftTests
{
    private sealed class TestLogger : ILogger
    {
        public readonly List<string> Warnings = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }

    [Fact]
    public async Task GetPendingMigrations_LogsWarning_WhenNameDrifts()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Seed history: version=1, but with OLD name that doesn't match "CreateBlogTable"
        // CreateBlogTable (v1) is already in the test assembly (SqliteMigrationRunnerTests)
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

        var logger = new TestLogger();
        // Use SqliteMigrationRunnerTests's assembly which contains CreateBlogTable (v1, "CreateBlogTable")
        var runner = new SqliteMigrationRunner(cn, typeof(SqliteMigrationRunnerTests).Assembly, logger: logger);

        await runner.GetPendingMigrationsAsync();

        // A drift warning should have been logged: history has "OldBlogMigration" but code has "CreateBlogTable"
        Assert.NotEmpty(logger.Warnings);
        Assert.Contains(logger.Warnings, w =>
            w.Contains("1") || w.Contains("CreateBlogTable") || w.Contains("OldBlogMigration"));
    }

    [Fact]
    public async Task GetPendingMigrations_NoWarning_WhenNameMatches()
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

        var logger = new TestLogger();
        var runner = new SqliteMigrationRunner(cn, typeof(SqliteMigrationRunnerTests).Assembly, logger: logger);

        await runner.GetPendingMigrationsAsync();

        // No drift warning (name matches for v1)
        Assert.Empty(logger.Warnings);
    }
}
