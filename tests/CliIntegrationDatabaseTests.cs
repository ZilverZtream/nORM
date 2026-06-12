using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using nORM.Cli;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    [Fact]
    public void Database_update_missing_assembly_returns_nonzero_without_leaking_connection_secret()
    {
        var root = FindRepositoryRoot();
        var secret = "TopSecret123!";
        var connection = $"Server=localhost;Database=norm;User ID=sa;Password={secret};Encrypt=True;TrustServerCertificate=True";
        var missingAssembly = Path.Combine(root, "missing-migrations.dll");

        var result = RunCli(
            $"database update --connection {Quote(connection)} --provider sqlserver --assembly {Quote(missingAssembly)}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Database_drop_sqlite_requires_yes_or_dry_run()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_guard_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        try
        {
            var result = RunCli(
                $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite",
                root);

            Assert.Equal(3, result.ExitCode);
            Assert.Contains("--yes", result.Stderr, StringComparison.Ordinal);
            Assert.True(File.Exists(dbFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public void Database_drop_sqlite_dry_run_does_not_delete_file()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_dry_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        try
        {
            var result = RunCli(
                $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite --dry-run",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("Would delete", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(dbFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public void Database_drop_sqlite_yes_deletes_file()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_yes_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        var result = RunCli(
            $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite --yes",
            root);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("deleted", result.Stdout, StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(dbFile));
    }

}
