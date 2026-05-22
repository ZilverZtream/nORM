using System;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public sealed class MigrationOptionsTests
{
    [Fact]
    public void MigrationOptions_defaults_match_runner_contract()
    {
        var options = new MigrationOptions();

        Assert.Equal("__NormMigrationsHistory", options.HistoryTableName);
        Assert.Equal("__NormMigrationsLock", options.LockName);
        Assert.Equal(TimeSpan.FromSeconds(30), options.LockTimeout);
        Assert.Equal(unchecked((long)0x62C3B8F921A4D507L), options.PostgresAdvisoryLockKey);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("123History")]
    [InlineData("History.Table")]
    [InlineData("History;DROP")]
    public void MigrationOptions_rejects_unsafe_history_table_names(string value)
    {
        Assert.ThrowsAny<ArgumentException>(() => new MigrationOptions(historyTableName: value));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("Lock'Name")]
    [InlineData("Lock;DROP")]
    public void MigrationOptions_rejects_unsafe_lock_names(string value)
    {
        Assert.ThrowsAny<ArgumentException>(() => new MigrationOptions(lockName: value));
    }

    [Fact]
    public async Task Sqlite_runner_uses_custom_history_table_name()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var options = new MigrationOptions(historyTableName: "CustomNormHistory");
        var runner = new SqliteMigrationRunner(connection, typeof(MigrationOptionsTests).Assembly, options);

        await runner.GetPendingMigrationsAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='CustomNormHistory'";
        var tableName = await cmd.ExecuteScalarAsync();

        Assert.Equal("CustomNormHistory", tableName);
    }
}
