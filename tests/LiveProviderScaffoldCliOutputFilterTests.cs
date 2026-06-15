#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public void Dotnet_norm_scaffold_enforces_output_safety_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveSafety" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_output_safety_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var baseCommand =
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSafetyCtx " +
                $"--table {Quote(tableName)}";

            var dryRun = RunCli(baseCommand + " --dry-run --json", root);
            Assert.True(dryRun.ExitCode == 0,
                $"CLI dry run failed with exit code {dryRun.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{dryRun.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{dryRun.Stderr}");
            using (var dryRunJson = JsonDocument.Parse(dryRun.Stdout))
            {
                var json = dryRunJson.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.True(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            Assert.False(Directory.Exists(output));

            var scaffold = RunCli(baseCommand, root);
            Assert.True(scaffold.ExitCode == 0,
                $"CLI scaffold failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveSafetyCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));
            var originalEntity = File.ReadAllText(entityPath);

            var noOverwrite = RunCli(baseCommand + " --no-overwrite", root);
            Assert.NotEqual(0, noOverwrite.ExitCode);
            Assert.Contains("already exists", noOverwrite.Stderr + noOverwrite.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(originalEntity, File.ReadAllText(entityPath));

            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md"), "# stale", Encoding.UTF8);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""", Encoding.UTF8);

            var force = RunCli(baseCommand + " --force --json", root);
            Assert.True(force.ExitCode == 0,
                $"CLI force scaffold failed with exit code {force.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{force.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{force.Stderr}");
            using (var forceJson = JsonDocument.Parse(force.Stdout))
            {
                var json = forceJson.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.False(json.GetProperty("dryRun").GetBoolean());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
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

            TryDeleteDirectory(output);
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
    public void Dotnet_norm_scaffold_accepts_csv_and_multi_value_table_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var csvTableOne = "CliLiveCsvOne" + suffix;
        var csvTableTwo = "CliLiveCsvTwo" + suffix;
        var multiTableOne = "CliLiveMultiOne" + suffix;
        var multiTableTwo = "CliLiveMultiTwo" + suffix;
        var skippedTable = "CliLiveFilterSkip" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_table_filter_values_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupTableFilterValuesScaffold(
                    connection,
                    provider,
                    kind,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTableFilterValuesCtx " +
                $"--tables {Quote("table:" + csvTableOne + ",table:" + csvTableTwo)} " +
                $"--table table:{multiTableOne} table:{multiTableTwo}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, csvTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, csvTableTwo + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableTwo + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveTableFilterValuesCtx.cs"));
            Assert.Contains($"IQueryable<{csvTableOne}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{csvTableTwo}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{multiTableOne}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{multiTableTwo}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupTableFilterValuesScaffold(
                    cleanup,
                    provider,
                    kind,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

}
