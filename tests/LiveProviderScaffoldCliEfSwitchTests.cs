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
    // Live CLI EF-compatible scaffold switch tests.

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_accepts_no_onconfiguring_data_annotations_and_no_pluralize_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfSwitch" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_switches_" + kind + "_" + suffix);
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

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveEfSwitchCtx " +
                "--no-onconfiguring " +
                "--data-annotations " +
                "--no-pluralize " +
                "--json " +
                "--verbose " +
                "--no-color " +
                "--prefix-output " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");
            Assert.True(string.IsNullOrWhiteSpace(scaffold.Stderr), scaffold.Stderr);

            using (var document = JsonDocument.Parse(scaffold.Stdout))
            {
                var json = document.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.False(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveEfSwitchCtx.cs"));
            Assert.DoesNotContain("OnConfiguring", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(connectionString, contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("[Table(\"", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Key]", entityCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
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

}
