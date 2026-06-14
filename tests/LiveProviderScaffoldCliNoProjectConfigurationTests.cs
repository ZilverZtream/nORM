#nullable enable

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_no_project_reads_named_connection_current_directory_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentDirConfig" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_dir_config_" + kind + "_" + suffix);
        var workDir = Path.Combine(tempRoot, "Work");
        var connectionName = "LiveCurrentDirConfig" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentDirConfigCtx " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(workDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(workDir, "Data", "Contexts", "CliLiveCurrentDirConfigCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Scaffolded;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Scaffolded.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Scaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, workDir);
            RunDotNet("build -c Release --nologo", workDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_no_project_pass_through_environment_selects_current_directory_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentDirEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_dir_env_" + kind + "_" + suffix);
        var workDir = Path.Combine(tempRoot, "Work");
        var connectionName = "LiveCurrentDirEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=CurrentDirectoryBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentDirEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(workDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(workDir, "Data", "Contexts", "CliLiveCurrentDirEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Scaffolded;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Scaffolded.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Scaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=CurrentDirectoryBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, workDir);
            RunDotNet("build -c Release --nologo", workDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }
}
